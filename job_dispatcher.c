#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <pthread.h>
#include <string.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>

#define MAX_COMMAND_LENGTH 50
//#define SERIAL

/*
result for example input file in problem assignment without WAIT with 4 processes (assuming you can't append existing output files and new ones need to be)
parallel = 7.53 
serial = 14.68
*/

typedef struct {
    int type;  
    int N;
    char name[9];
}server_request;

typedef struct node{
    char command[MAX_COMMAND_LENGTH];
    double time;
    int type;
    struct node *next;
}queue_node;

enum {PRIMES,PRIMEDIVISORS,ANAGRAMS,END};
enum {RECEIVED,DISPATCHED,FINISHED};
int *occupied_servers = NULL;
char *commands_per_server = NULL;
int comm_sz,end =0;
double time_start;
sem_t sem;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
queue_node *head = NULL;
queue_node *tail = NULL;
pthread_cond_t not_empty;

int factorial(int n){
    int result = 1;
    for(int i=2;i<=n;i++)
        result *= i;
    return result;
}

int is_prime(int n){
    for(int i=2;i*i <= n; i++)
        if(n % i == 0)
            return 0;
    
    return 1;
}

int primes(int n){
    int result = 0;
    for(int i=2; i<=n; i++){
        if(is_prime(i))
            result++;
    }
    return result;
}

int primedivisors(int n){
    int result=0;
    int a = 2;
    while(n > 1){
        if(n % a == 0){
            result++;
            n /= a;
        }
        else
            a++;
    }
    return result;
}

void swap(char *x, char *y){
    char temp = *x;
    *x = *y;
    *y = temp;
}

void permute(char *temp, char *name, int l, int r){
    if (l == r)
        sprintf(temp+strlen(temp),"%s\n",name);
    else{
        for(int i=l; i <= r; i++){
            swap((name + l),(name + i));
            permute(temp,name,l+1,r);
            swap((name + l),(name + i));
        }
    }
}

char *anagrams(char *name){  
    int n = strlen(name);
    char *temp;
    temp = (char*)malloc(sizeof(char) * (n+1) * (factorial(n)));
    if(temp == NULL){
        perror("memory allocation");
        MPI_Abort(MPI_COMM_WORLD,2);
    }
    temp[0] = '\0';
    permute(temp,name,0,n-1);

    return temp;
}

int isEmpty(){
    return (head == NULL);
}

void enqueue(char command[],struct timespec t,int type){
    double time = t.tv_sec + t.tv_nsec / 1000000000.0;
    time -= time_start;    
    struct node *new = malloc(sizeof(struct node));
    
    pthread_mutex_lock(&queue_mutex);
    strcpy(new->command,command);
    new->time = time;
    new->type = type;
    new->next = NULL;

    if (!isEmpty())
    {
        tail->next = new;
        tail = new;
    }
    else
    {
        head = new;
        tail = new;
    }

    pthread_cond_signal(&not_empty);
    pthread_mutex_unlock(&queue_mutex);
}

void *log_function(void *arg){
    FILE *log_file = (FILE *)arg;
    struct node *oldhead;

    while(end != 2){
        pthread_mutex_lock(&queue_mutex);

        while (isEmpty() && end != 2)
            pthread_cond_wait(&not_empty, &queue_mutex);

        if(end == 2){
            pthread_mutex_unlock(&queue_mutex);
            return NULL;
        }

        oldhead = head;
        head = head->next;
        if (head == NULL)
            tail = NULL;
        	
        pthread_mutex_unlock(&queue_mutex);

        switch(oldhead->type){
            case RECEIVED:
                fprintf(log_file,"%f: %s RECEIVED\n",oldhead->time,oldhead->command);
                break;
            case DISPATCHED:
                fprintf(log_file,"%f: %s DISPATCHED\n",oldhead->time,oldhead->command);
                break;
            case FINISHED:
                fprintf(log_file,"%f: %s FINISHED\n",oldhead->time,oldhead->command);
                break;
        }
        free(oldhead);
    }

    return NULL;
}

void *receive_results(void *arg){
    FILE *out;
    int result;
//    int count;
    int sem_value=0;
    char output_path[20],*anagrams_result = NULL;
    MPI_Status status;
    struct timespec time;

    while(!end || sem_value != comm_sz-1){
        MPI_Probe(MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
        clock_gettime(CLOCK_MONOTONIC,&time);
        enqueue(&commands_per_server[status.MPI_SOURCE * MAX_COMMAND_LENGTH],time,FINISHED);

        sprintf(output_path,"output_client%d.txt",occupied_servers[status.MPI_SOURCE]);


        switch(status.MPI_TAG){
            case PRIMES:
            case PRIMEDIVISORS:
                MPI_Recv(&result,1,MPI_INT,status.MPI_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                break;
            case ANAGRAMS:
//                MPI_Get_count(&status,MPI_INT,&count);
                anagrams_result = (char*)malloc(status._ucount + 1);
                MPI_Recv(anagrams_result,status._ucount,MPI_CHAR,status.MPI_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                anagrams_result[status._ucount] = '\0';                
                break;
        }
        
        occupied_servers[status.MPI_SOURCE] = -1;
        sem_post(&sem);

        if((out = fopen(output_path,"a")) == NULL){
            perror("opening output");
            MPI_Abort(MPI_COMM_WORLD,1);
        }
        if(status.MPI_TAG == PRIMES || status.MPI_TAG == ANAGRAMS){
            fprintf(out,"%d\n",result);
        }
        else{
            fprintf(out,"%s\n",anagrams_result);
            free(anagrams_result);
        }

        fclose(out);
        sem_getvalue(&sem,&sem_value);
    }

    end = 2;
    pthread_cond_signal(&not_empty);

    return NULL;
}


void send_request(server_request request, MPI_Datatype MPI_server_request,int client_id,char original_command[]){
    sem_wait(&sem);
    for(int i=1; i<comm_sz; i++){
        if(occupied_servers[i] == -1){
            MPI_Send(&request,1,MPI_server_request,i,0,MPI_COMM_WORLD);
            occupied_servers[i] = client_id;
            strcpy(&commands_per_server[i*MAX_COMMAND_LENGTH],original_command);
            break;
        }
    }
}

void main_server(FILE *f, FILE *log_file, MPI_Datatype MPI_server_request){
    char command[MAX_COMMAND_LENGTH],original_command[MAX_COMMAND_LENGTH];
    char *token,*request_type,*request_argument;
    int sleep_amount,unknown_command = 0;        
    pthread_t th[2];
    server_request request;
    struct timespec time;

    pthread_mutex_init(&queue_mutex,NULL);
    pthread_cond_init(&not_empty,NULL);
    sem_init(&sem,0,comm_sz-1);

    occupied_servers = (int*)malloc((comm_sz+1) * sizeof(int));
    if(occupied_servers == NULL){
        perror("memory allocation");
        MPI_Abort(MPI_COMM_WORLD,2);
    }
    commands_per_server = (char*)malloc((comm_sz+1) * MAX_COMMAND_LENGTH * sizeof(char));
    if(occupied_servers == NULL){
        perror("memory allocation");
        MPI_Abort(MPI_COMM_WORLD,2);
    }

    memset(occupied_servers,-1,(comm_sz+1) * sizeof(int));

    if(pthread_create(&th[0],NULL,receive_results,NULL) != 0){
        perror("creating thread");
        MPI_Abort(MPI_COMM_WORLD,3);
    }

    if(pthread_create(&th[1],NULL,log_function,(void *)log_file) != 0){
        perror("creating thread");
        MPI_Abort(MPI_COMM_WORLD,3);
    }

    while(fgets(command,49,f) != NULL){
        clock_gettime(CLOCK_MONOTONIC,&time);

        if(strcmp(command,"\n") == 0)
            continue;

        command[strlen(command)-1] = '\0';
        strcpy(original_command,command);
        
        token = strtok(command," ");
        if(strcmp(token, "WAIT") == 0){
            token =  strtok(NULL," ");
            sleep_amount = atoi(token);
            sleep(sleep_amount);
        }
        else{
            enqueue(original_command,time,RECEIVED);
            request_type = strtok(NULL," ");
            request_argument = strtok(NULL, " ");
            if(strcmp(request_type,"PRIMES") == 0){
                request.type = PRIMES;
                request.N = atoi(request_argument);
                request.name[0] = '\0';
            }
            else if(strcmp(request_type,"PRIMEDIVISORS") == 0){
                request.type = PRIMEDIVISORS;
                request.N = atoi(request_argument);
                request.name[0] = '\0';               
            }
            else if(strcmp(request_type,"ANAGRAMS") == 0){
                request.type = ANAGRAMS;    
                request.N = 0;
                request_argument[strlen(request_argument)-1] = '\0';
                strncpy(request.name,request_argument,8);
                request.name[8]='\0';               
            }
            else{
                unknown_command = 1;
                printf("Unknown command %s %s %s\n",token,request_type,request_argument);
                fflush(stdout);
            }

            if(!unknown_command){
                send_request(request,MPI_server_request,atoi(token+3),original_command);
                clock_gettime(CLOCK_MONOTONIC,&time);
                enqueue(original_command,time,DISPATCHED);
            }
        }
    }

    end = 1;
    request.type = END;
    for(int i = 1;i < comm_sz;i++)
        MPI_Send(&request,1,MPI_server_request,i,0,MPI_COMM_WORLD);

    pthread_join(th[0],NULL);
    pthread_join(th[1],NULL);
    sem_destroy(&sem);
    pthread_mutex_destroy(&queue_mutex);
    pthread_cond_destroy(&not_empty);
    free(occupied_servers);
    free(commands_per_server);
}

void worker_server(MPI_Datatype MPI_server_request,int my_rank){
    server_request request;
    int end = 0, result=0;
    char *anagrams_result = NULL;
    long int nr_bytes = 0;

    while(!end){
        MPI_Recv(&request,1,MPI_server_request,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);

        switch(request.type){
            case PRIMES:
                result = primes(request.N);
                break;
            case PRIMEDIVISORS:
                result = primedivisors(request.N);
                break;
            case ANAGRAMS:
                nr_bytes = factorial(strlen(request.name)) * (strlen(request.name) + 1) * sizeof(char);
                anagrams_result = anagrams(request.name);
                break;
            case END:
                result = 0;
                end = 1;
                break;
        }

        if(end)
            break;

        if(request.type == ANAGRAMS){
            MPI_Send(anagrams_result,nr_bytes,MPI_CHAR,0,ANAGRAMS,MPI_COMM_WORLD);
            free(anagrams_result);        
        }
        else
            MPI_Send(&result,1,MPI_INT,0,request.type,MPI_COMM_WORLD);

    }
}

void serial_job_dispatcher(FILE *f, FILE *log_file, double time_start){
    char command[MAX_COMMAND_LENGTH],original_command[MAX_COMMAND_LENGTH],output_path[100];
    char *token,*request_type,*request_argument,*anagrams_result;
    int sleep_amount;        
    struct timespec time;
    double time_double;
    int n;
    FILE *out;
    

    while(fgets(command,49,f) != NULL){
        clock_gettime(CLOCK_MONOTONIC,&time);

        if(strcmp(command,"\n") == 0)
            continue;

        command[strlen(command)-1] = '\0';
        strcpy(original_command,command);
        
        token = strtok(command," ");
        if(strcmp(token, "WAIT") == 0){
            token =  strtok(NULL," ");
            sleep_amount = atoi(token);
            sleep(sleep_amount);
        }
        else{
            time_double = time.tv_sec + time.tv_nsec / 1000000000.0;
            time_double -= time_start;  
            fprintf(log_file,"%f: %s RECEIVED\n",time_double,original_command);
            request_type = strtok(NULL," ");
            request_argument = strtok(NULL, " ");
            
            sprintf(output_path,"output_client_serial%d.txt",atoi(token+3));
            if((out = fopen(output_path,"a")) == NULL){
                perror("opening output");
                MPI_Abort(MPI_COMM_WORLD,1);
            }
            
            if(strcmp(request_type,"PRIMES") == 0){
                clock_gettime(CLOCK_MONOTONIC,&time);
                time_double = time.tv_sec + time.tv_nsec / 1000000000.0;
                time_double -= time_start;  
                fprintf(log_file,"%f: %s STARTED\n",time_double,original_command);
                n = primes(atoi(request_argument));
                fprintf(out,"%d\n",n);
                clock_gettime(CLOCK_MONOTONIC,&time);
                time_double = time.tv_sec + time.tv_nsec / 1000000000.0;
                time_double -= time_start;
                fprintf(log_file,"%f: %s FINISHED\n",time_double,original_command);
            }
            else if(strcmp(request_type,"PRIMEDIVISORS") == 0){
                clock_gettime(CLOCK_MONOTONIC,&time);
                time_double = time.tv_sec + time.tv_nsec / 1000000000.0;
                time_double -= time_start;  
                fprintf(log_file,"%f: %s STARTED\n",time_double,original_command);
                n = primedivisors(atoi(request_argument));
                fprintf(out,"%d\n",n);
                clock_gettime(CLOCK_MONOTONIC,&time);
                time_double = time.tv_sec + time.tv_nsec / 1000000000.0;
                time_double -= time_start;
                fprintf(log_file,"%f: %s FINISHED\n",time_double,original_command);            
            }
            else if(strcmp(request_type,"ANAGRAMS") == 0){
                request_argument[strlen(request_argument)-1] = '\0';
                clock_gettime(CLOCK_MONOTONIC,&time);
                time_double = time.tv_sec + time.tv_nsec / 1000000000.0;
                time_double -= time_start;  
                fprintf(log_file,"%f: %s STARTED\n",time_double,original_command);    
                anagrams_result = anagrams(request_argument);
                fprintf(out,"%s\n",anagrams_result);
                clock_gettime(CLOCK_MONOTONIC,&time);
                time_double = time.tv_sec + time.tv_nsec / 1000000000.0;
                time_double -= time_start;
                fprintf(log_file,"%f: %s FINISHED\n",time_double,original_command);
                free(anagrams_result);
            }
            else{
                printf("Unknown command %s %s %s\n",token,request_type,request_argument);
                fflush(stdout);
            }
            fclose(out);
        }
    }
}

int main(int argc, char **argv){
    int my_rank, block_counts[3];
    FILE *f,*log_file;
    MPI_Datatype MPI_server_request,old_types[3];
    MPI_Aint offsets[3];
    struct timespec start,finish;
    double total_time;

    if(argc < 2){
        printf("No input file\n");
        exit(1);
    }


    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    offsets[0] = offsetof(server_request,type);
    old_types[0] = MPI_INT;
    block_counts[0] = 1;

    offsets[1] = offsetof(server_request,N);
    old_types[1] = MPI_INT;
    block_counts[1] = 1;

    offsets[2] = offsetof(server_request,name);
    old_types[2] = MPI_CHAR;
    block_counts[2] = 9;

    MPI_Type_create_struct(3,block_counts,offsets,old_types,&MPI_server_request);
    MPI_Type_commit(&MPI_server_request);

    if(my_rank == 0){           
        if((f = fopen(argv[1],"r")) == NULL){
            printf("Problem opening input file\n");
            MPI_Abort(MPI_COMM_WORLD,1);
        }
        if((log_file = fopen("log.txt","w")) == NULL){
            printf("Problem opening output file\n");
            MPI_Abort(MPI_COMM_WORLD,1);
        }
        clock_gettime(CLOCK_MONOTONIC,&start);
        time_start = start.tv_sec + start.tv_nsec / 1000000000.0;
        main_server(f,log_file,MPI_server_request);        
        clock_gettime(CLOCK_MONOTONIC,&finish);
        total_time = (finish.tv_sec - start.tv_sec);
        total_time += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
        printf("Parallel time: %f\n",total_time);        
        fclose(f);
        fclose(log_file);
    }
    else
        worker_server(MPI_server_request,my_rank);

#ifdef SERIAL
    if(my_rank == 0){
        if((f = fopen(argv[1],"r")) == NULL){
            printf("Problem opening input file\n");
            MPI_Abort(MPI_COMM_WORLD,1);
        }
        if((log_file = fopen("log_serial.txt","w")) == NULL){
            printf("Problem opening output file\n");
            MPI_Abort(MPI_COMM_WORLD,1);
        }
        rewind(f);
        clock_gettime(CLOCK_MONOTONIC,&start);
        time_start = start.tv_sec + start.tv_nsec / 1000000000.0;
        serial_job_dispatcher(f,log_file,time_start);
        clock_gettime(CLOCK_MONOTONIC,&finish);
        total_time = (finish.tv_sec - start.tv_sec);
        total_time += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
        printf("Serial time: %f\n",total_time);        
        fclose(f);
        fclose(log_file);
    }
#endif
    
    MPI_Type_free(&MPI_server_request);
    MPI_Finalize();
    
    return 0;
}

