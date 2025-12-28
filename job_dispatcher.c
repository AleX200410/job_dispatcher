#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <pthread.h>
#include <string.h>
#include <semaphore.h>
#include <unistd.h>

enum {PRIMES,PRIMEDIVISORS,ANAGRAMS,END};
int *occupied_servers = NULL;
int comm_sz,end =0;
sem_t sem;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int type;  
    int N;
    char name[9];
}server_request;

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

char *anagrams(char *name){  //TO DO: fix, doesn't work, 
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

void *receive_results(void *arg){
//    FILE *log_file = (FILE *)arg;
    FILE *out;
    int result;
//    int count;
    int sem_value=0;
    char output_path[20],*anagrams_result = NULL;
    MPI_Status status;

    while(!end || sem_value != comm_sz-1){
        MPI_Probe(MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
        printf("doing stuff\n");
        fflush(stdout);

        sprintf(output_path,"output_client%d.txt",occupied_servers[status.MPI_SOURCE]);
        if((out = fopen(output_path,"a")) == NULL){
            perror("opening output");
            MPI_Abort(MPI_COMM_WORLD,1);
        }

        switch(status.MPI_TAG){
            case PRIMES:
            case PRIMEDIVISORS:
                MPI_Recv(&result,1,MPI_INT,status.MPI_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                fprintf(out,"%d\n",result);
                break;
            case ANAGRAMS:
//                MPI_Get_count(&status,MPI_INT,&count);
//                printf("anagram count %ld\n",status._ucount);
                anagrams_result = (char*)malloc(status._ucount + 1);
                MPI_Recv(anagrams_result,status._ucount,MPI_CHAR,status.MPI_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                anagrams_result[status._ucount] = '\0';
                fprintf(out,"%s\n",anagrams_result);
                free(anagrams_result);
                break;
        }
        
        occupied_servers[status.MPI_SOURCE] = -1;
        sem_post(&sem);
        fclose(out);
        sem_getvalue(&sem,&sem_value);
    }

    return NULL;
}

void send_request(server_request request, MPI_Datatype MPI_server_request,int client_id){
    sem_wait(&sem);
    for(int i=1; i<comm_sz; i++){
        if(occupied_servers[i] == -1){
            MPI_Send(&request,1,MPI_server_request,i,0,MPI_COMM_WORLD);
            occupied_servers[i] = client_id;
            break;
        }
    }
}

void main_server(FILE *f, FILE *log_file, MPI_Datatype MPI_server_request){
    char command[100];
    char *token,*request_type,*request_argument;
    int sleep_amount;        
    pthread_t th;
    server_request request;

    pthread_mutex_init(&mutex,NULL);
    sem_init(&sem,0,comm_sz-1);

    occupied_servers = (int*)malloc((comm_sz+1) * sizeof(int));
    if(occupied_servers == NULL){
        perror("memory allocation");
        MPI_Abort(MPI_COMM_WORLD,2);
    }
    memset(occupied_servers,-1,(comm_sz+1) * sizeof(int));

    if(pthread_create(&th,NULL,receive_results,(void *)log_file) != 0){
        perror("creating thread");
        MPI_Abort(MPI_COMM_WORLD,3);
    }

    while(fgets(command,99,f) != NULL){
        token = strtok(command," ");
        if(strcmp(token, "WAIT") == 0){
            token =  strtok(NULL," ");
            sleep_amount = atoi(token);
            sleep(sleep_amount);
        }
        else{
            request_type = strtok(NULL," ");
            request_argument = strtok(NULL, " ");
            if(strcmp(request_type,"PRIMES") == 0){
                request.type = PRIMES;
                request.N = atoi(request_argument);
                request.name[0] = '\0';
                send_request(request,MPI_server_request,atoi(token+3));
            }
            else if(strcmp(request_type,"PRIMEDIVISORS") == 0){
                request.type = PRIMEDIVISORS;
                request.N = atoi(request_argument);
                request.name[0] = '\0';
                send_request(request,MPI_server_request,atoi(token+3));
            }
            else if(strcmp(request_type,"ANAGRAMS") == 0){
                request.type = ANAGRAMS;    
                request.N = 0;
                request_argument[strlen(request_argument)-1] = '\0';
                strncpy(request.name,request_argument,8);
                request.name[8]='\0';
                send_request(request,MPI_server_request,atoi(token+3));
            }
            else{
                printf("Unknown command %s %s %s\n",token,request_type,request_argument);
                fflush(stdout);
            }
        }
    }

    end = 1;
    request.type = END;
    for(int i = 1;i < comm_sz;i++)
        MPI_Send(&request,1,MPI_server_request,i,0,MPI_COMM_WORLD);

    pthread_join(th,NULL);
    sem_destroy(&sem);
    free(occupied_servers);
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

int main(int argc, char **argv){
    int my_rank, block_counts[3];
    FILE *f,*log_file;
    MPI_Datatype MPI_server_request,old_types[3];
    MPI_Aint offsets[3];

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
        main_server(f,log_file,MPI_server_request);
        fclose(f);
        fclose(log_file);
    }
    else
        worker_server(MPI_server_request,my_rank);

    printf("succes exit yay %d\n",my_rank);
    MPI_Type_free(&MPI_server_request);
    MPI_Finalize();
    
    return 0;
}