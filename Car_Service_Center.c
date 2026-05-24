#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <semaphore.h>

#define K 20                  // Total number of cars
#define N 10                  // Institute capacity 
#define POLLUTION_QUEUE_SIZE 3 // Max cars allowed to wait for pollution test 
#define Regular 0
#define VIP 1
#define MECHANIC_TOOL 2

#define CASHIER  1            // Number of cashier workers 
#define MECHANICS 3           // Number of mechanics workers 
#define TESTER 2              // Number of testers workers 
#define MUTEX 1               // Used as a binary semaphore value


typedef struct CarNode CarNode;


 // Car node represents a single car job that moves through the system.
struct CarNode{
    int id;                         // Car ID 
    int is_vip;                     // VIP flag regular = 0 else vip = 1
    struct timeval thread_create_time; // Timestamp when pushed into a queue 
    struct CarNode *next;           // Next node in a linked list queue
};



typedef struct
{
    CarNode *head;
    CarNode *tail;
} Queue;


// Add to end of queue
void enqueue(Queue *q, CarNode* car)
{
    car->next = NULL;
    if (q->tail)
        q->tail->next = car;
    else
        q->head = car;
    q->tail = car;
}

// Remove from start of queue, return 1 if success, 0 if empty
int dequeue(Queue *q, CarNode **car)
{
    if (!q->head)
        return 0;

    CarNode *tmp = q->head;
    q->head = tmp->next;

    if (!q->head)
        q->tail = NULL;

    tmp->next = NULL;
    *car = tmp;
    return 1;
}

// Returns 1 if queue is empty, else 0
int is_empty(Queue *q){
    if(q->head == NULL)
       return 1;
    return 0;
}



// Tablets: 2 available
// Torque wrenches: 2 available
sem_t sem_tablet, sem_torque_wrench;


// Worker capacity semaphores:
// mechanics 3 workers
// testers 2 pollu workers
sem_t sem_pollu_workers, sem_mech_worker;


//Queue mutexes for protecting queue operations.
//Each queue group has a semaphore used like a mutex.
sem_t sem_Q_tester, sem_Q_mech, sem_Q_cashier;



// sem_carQ limits number of cars that can be waiting for pollution test.
sem_t sem_carQ;



// sem_institute limits number of cars that can be inside the institute at the same time 
sem_t sem_institute;


//One cashier  availability
sem_t sem_cashier;


// Protects shared variable end_programe
sem_t mutex_cars;

// Number of cars that have completed payment
int end_programe;

// ----------------------------
// Global state: cars and stage queues (VIP + Regular for each stage)
// ----------------------------


// cars[] array
CarNode cars[K];

// Queues: mechanics, testers, cashier, each with VIP and Regular queues
Queue vip_cars_mech = {NULL,NULL};
Queue regular_cars_mech = {NULL,NULL};

Queue vip_cars_tester = {NULL,NULL};
Queue regular_cars_tester = {NULL,NULL};

Queue vip_cars_cashier = {NULL,NULL};
Queue regular_cars_cashier = {NULL,NULL};

// Thread function declarations
void* car_thread(void* argv);
void push_car(Queue *vip_cars,Queue *regular_cars,CarNode *car,sem_t *sem_worker);
int pop_car(Queue *vip_cars,Queue *regular_cars,CarNode **car,sem_t *sem_worker);
void* mechanics(void* argv);
void* airPollution(void* argv);
void* payment(void* argv);


// ----------------------------
// Car thread print arrival and type of thr car (vip/reagulr) + push into first mechanic VIP or Regular queue
// ----------------------------
void* car_thread(void* argv){

    int i = *((int *)argv); // Car index in cars[]
   
    //Institute capacity if full, car waits until someone exits 
    sem_wait(&sem_institute);

    // Print arrival message
    if(cars[i].is_vip){
        printf("[    Main #%d] Arrived (VIP)\n",i+1);
    }else{
        printf("[   Main #%d] Arrived (Regular)\n",i+1);
    }

    // After the LAST car is generated, print.
    if(i == K-1)
       printf("--- All cars generated. Waiting for complrtion... ---\n");

    // Push car into mechanic VIP or Regular queue
    push_car(&vip_cars_mech,&regular_cars_mech,&cars[i],&sem_Q_mech);

    return NULL;
}

// ----------------------------
// Push a car into a queue (VIP or Regular)
// ----------------------------
void push_car(Queue *vip_cars,Queue *regular_cars,CarNode* car,sem_t *sem_worker){
     // Use sem_worker as a mutex to protect queue operations
     sem_wait(sem_worker);

     // Save time of entering the queue 
     gettimeofday(&car->thread_create_time, NULL);

     // Enqueue to the appropriate queue
     if(car->is_vip)
       enqueue(vip_cars,car);
     else
       enqueue(regular_cars,car);

     // Release the mutex
     sem_post(sem_worker);
}

// Pop a car from a queue and check
//if Regular cars that waited >= 400ms will promoted to VIP.
//Always pop from VIP first, else Regular.
//Returns 1 if a car was popped, 0 if both queues are empty.
int pop_car(Queue *vip_cars,Queue *regular_cars,CarNode **car,sem_t *sem_worker){

   struct timeval now;
   double elapsedTime;

   // Lock queue group
   sem_wait(sem_worker);

   CarNode *head,*moved_car;

   // Aging check while the oldest Regular waited long enough, promote it to VIP
   while(!is_empty(regular_cars)){
        head = regular_cars->head;
        
        gettimeofday(&now, NULL);

        // Compute elapsed time in milliseconds
        elapsedTime = (now.tv_sec - head->thread_create_time.tv_sec) * 1000.0;
        elapsedTime += (now.tv_usec - head->thread_create_time.tv_usec) / 1000.0;

        // Promote to VIP if waited >= 400ms
        if(elapsedTime >= 400.0){
           printf("[   System #%d] Aging Alert! Waited %.0fms -> Promoted to VIP \n",
                  head->id, elapsedTime);

           if(!dequeue(regular_cars,&moved_car))
              break;

           moved_car->is_vip = 1;
           enqueue(vip_cars,moved_car);

        }else{
           // If the first Regular hasn't waited long enough, stop scanning
           break;
        }
   }

   int ok = 0;

   // VIP has priority
   if(!is_empty(vip_cars)){
       ok = dequeue(vip_cars,car);
   }
   else if(!is_empty(regular_cars)){
       ok = dequeue(regular_cars,car);
   }

   // Unlock queue group
   sem_post(sem_worker);

   return ok;
}

// ----------------------------
// Mechanics worker thread
// ----------------------------
void* mechanics(void* argv){

   int mechanic_index = *((int*)argv); // Mechanic index 
   CarNode *car;
   int ok;

   // Keep working until all K cars have completed payment
   while(end_programe < K){

      // pop a car from mechanics queues
      ok = pop_car(&vip_cars_mech,&regular_cars_mech,&car,&sem_Q_mech);
      //if queues not empty
      if(ok){

         // Random mechanic processing time 100ms to 200ms for simulate work
         int working_time = 100000 + (rand() % 100001);

         // Acquire a mechanic worker slot
         sem_wait(&sem_mech_worker);

         // Acquire tools 
         sem_wait(&sem_tablet);
         sem_wait(&sem_torque_wrench);

         printf("[   Mech #%d] Mech-%d: Working (Tools Acquired)\n", car->id, mechanic_index+1);

         // Simulate work
         usleep(working_time);

         // Return tools
         sem_post(&sem_torque_wrench);
         sem_post(&sem_tablet);

         // Pollution queue capacity wait until there is room
         sem_wait(&sem_carQ);

         // Push to testers queue
         push_car(&vip_cars_tester,&regular_cars_tester,car,&sem_Q_tester);

         printf("[   Mech #%d] Mech-%d: Finished & Returned Tools\n", car->id, mechanic_index+1);
         printf("[   Mech #%d] Mech-%d: Moved Car to Pollution Queue\n", car->id, mechanic_index+1);

         // Release mechanic worker slot
         sem_post(&sem_mech_worker);
      }
   }

   return NULL;
}

// ----------------------------
// Air pollution tester worker thread
// ----------------------------
void* airPollution(void* argv){

   int tester_index  = *((int*)argv); // Tester index 
   int ok;
   CarNode *car;

   // Keep working until all K cars have completed payment
   while(end_programe < K){

      //  pop a car from tester queues
      ok = pop_car(&vip_cars_tester,&regular_cars_tester,&car,&sem_Q_tester);
      //if queues not empty
      if(ok){

         // Car leaves the pollution waiting queue 
         sem_post(&sem_carQ);

         // Acquire a tester worker slot
         sem_wait(&sem_pollu_workers);

         printf("[   Tester #%d] Tester-%d: Starting Pollution Test\n", car->id, tester_index+1);

         // Simulate pollution test time 150ms
         usleep(150000);

         printf("[   Tester #%d] Tester-%d: Done. Left Institute.\n", car->id, tester_index+1);

         // Push to cashier queues for payment
         push_car(&vip_cars_cashier,&regular_cars_cashier,car,&sem_Q_cashier);

         // Release tester worker slot
         sem_post(&sem_pollu_workers);
      }
   }

   return NULL;
}

// ----------------------------
// Cashier worker thread (payment)
// ----------------------------
void* payment(void* argv){
   CarNode *car;
   int ok;

   // Keep working until all K cars have completed payment
   while(end_programe < K){

      // Try to pop a car from cashier queues
      ok = pop_car(&vip_cars_cashier,&regular_cars_cashier,&car,&sem_Q_cashier);
      //if queues not empty
      if(ok){

         // Acquire cashier
         sem_wait(&sem_cashier);

         // Simulate payment time 50ms
         usleep(50000);

         // Release cashier
         sem_post(&sem_cashier);

         // Increment completion counter safely
         sem_wait(&mutex_cars);
         end_programe++;
         sem_post(&mutex_cars);

         // Car exits institute free capacity institute for a new arriving car
         sem_post(&sem_institute);
      }
   }

   return NULL;
}




int main(){

    srand(time(NULL));

    pthread_t thread_cars[K];
    pthread_t mechanics_thread[MECHANICS];
    pthread_t testers_thread[TESTER];
    pthread_t cashier_thread;
    pthread_t thread_status;

    // Initialize mechanic tool semaphores 
    sem_init(&sem_tablet, 0, MECHANIC_TOOL);
    sem_init(&sem_torque_wrench, 0, MECHANIC_TOOL);

    // Initialize worker availability semaphores
    sem_init(&sem_pollu_workers, 0, TESTER);
    sem_init(&sem_mech_worker, 0, MECHANICS);

    // Bounded queue capacity for pollution stage waiting
    sem_init(&sem_carQ, 0, POLLUTION_QUEUE_SIZE);

    // Institute capacity 
    sem_init(&sem_institute, 0, N);

    // Queue mutexes
    sem_init(&sem_Q_mech,0,MUTEX);
    sem_init(&sem_Q_tester,0,MUTEX);
    sem_init(&sem_Q_cashier,0,MUTEX);

    // Cashier semaphore and completion mutex
    sem_init(&sem_cashier, 0, CASHIER);
    sem_init(&mutex_cars, 0, MUTEX);

    int r;
    int index_car[K], index_mech[MECHANICS], index_tester[TESTER]; 
    end_programe = 0;

    printf("--- Institute Open (N=%d, K=%d) ---\n", N, K);

    // Create mechanics worker threads
    for(int i = 0; i < MECHANICS; i++){
       index_mech[i] = i;
       thread_status = pthread_create(&mechanics_thread[i], NULL, mechanics, &index_mech[i]);
       if(thread_status != 0){
           perror("Error creating thread");
           exit(1);
       }
    }

    // Create tester worker threads
    for(int i = 0; i < TESTER; i++){
       index_tester[i] = i;
       thread_status = pthread_create(&testers_thread[i], NULL, airPollution, &index_tester[i]);
       if(thread_status != 0){
           perror("Error creating thread");
           exit(1);
       }
    }

    // Create cashier thread
    thread_status = pthread_create(&cashier_thread,  NULL, payment, NULL);
    if(thread_status != 0){
        perror("Error creating thread");
        exit(1);
    }

    // Generate K car threads
    for(int i = 0; i < K; i++){
        usleep(50000);//create car every 0.05s

        r = rand()%4;//25% VIP probability 
        if(r == 0){
            cars[i].is_vip = VIP;
        }else{
            cars[i].is_vip = Regular;
        }

        cars[i].id = i + 1;
        cars[i].next = NULL;

        index_car[i] = i;

        thread_status = pthread_create(&thread_cars[i], NULL, car_thread, &index_car[i]);
        if(thread_status != 0){
            perror("Error creating thread");
            exit(1);
        }
    }

    // Join all car threads 
    for(int i = 0; i < K; i++){
        thread_status = pthread_join(thread_cars[i], NULL);
        if(thread_status != 0) {
          perror("Error joining");
          exit(1);
        }
    }

    // Join mechanics threads 
    for(int i = 0; i < MECHANICS; i++){
        thread_status = pthread_join(mechanics_thread[i], NULL);
        if(thread_status != 0){
            perror("Error joining");
            exit(1);
        }
    }

    // Join tester threads
    for(int i = 0; i < TESTER; i++){
        thread_status = pthread_join(testers_thread[i], NULL);
        if(thread_status != 0){
            perror("Error joining");
            exit(1);
        }
    }

    // Join cashier thread
    thread_status = pthread_join(cashier_thread, NULL);
    if(thread_status != 0) {
      perror("Error joining");
      exit(1);
    }

    return 0;
}

