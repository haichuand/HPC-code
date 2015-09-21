/*
  File: tmsort.c
  Compile: gcc tmort.c -o tmsort -lpthread
  Use: tmsort N
       [N is the number of elements in random list to be sorted]
       Uses one thread to do merge sort

   or: tmsort N P
       [N is the number of elements in random list to be sorted]
       [P is the number of threads, must be power of 2]

  Description: generates and sorts a list of random integers
               using merge sort; checks if list is sorted

 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <stdint.h>

int N, P;
int nPerThread; //number count by each thread
pthread_t* tPointer; //pointer to pthread_t array
int *x; //pointer to integer array for sorting
int allDone; //flag for all threads created

void mergeSort(int *, int);
void merge(int *, int, int *, int);
void checkSort(int *, int);
void allSort(int *, int);
void* tMergeSort(void *);
void* tMergeSortLast(void *);



int main(int argc, char **argv) {
    int i;
    clock_t t1, t2;


    if (argc != 2 && argc != 3) {
        printf("Usage: mSort [# elements] [optional: # threads]\n");
        exit(0);
    }

    N = atoi(argv[1]);
    x = (int *) calloc(N, sizeof (int));

    for (i = 0; i < N; i++) {
        x[i] = random();
    }

    if (argc == 3) {
        P = atoi(argv[2]);
    } else {
        P = 1;
    }
    printf("Sorting %d integers in %d threads\n", N, P);

    int k;
    for (k=P; k%2==0; k=k/2);
    if (k!=1) {
        printf ("Error: thread number must be power of 2\n");
        exit(0);
    }
    nPerThread = N / P;
    checkSort(x, N);

    t1 = clock(); /* get timestamp before sort */
    if (P==1)
        mergeSort(x, N);
    else
        allSort(x, N);

    t2 = clock(); /* get timestamp after sort */
    printf("Elapsed: %.4f seconds\n", (t2 - t1) / (double) CLOCKS_PER_SEC);

    checkSort(x, N);
    return 0;
}

/* creates P threads, each merge-sorts a chunk of the list,
 * then does a parallel binary merge */
void allSort(int *x, int n) {
    allDone = 0;
    tPointer = (pthread_t*) calloc(P, sizeof (pthread_t));
    int i=0;
    for (i=0; i<P-1; i++) {
        if (pthread_create(&tPointer[i], NULL, tMergeSort, (void*)(intptr_t) i)) {
                printf("Error creating thread %d.\n", i);
                exit(0);
            }
    }

    if (pthread_create(&tPointer[P-1], NULL, tMergeSortLast, NULL)) {
        printf("Error creating thread %d.\n", P-1);
        exit(0);
    }
    allDone = 1;

    if (pthread_join(tPointer[0], NULL)) {
        printf("Error exiting thread 0 \n");
        exit(0);
    }
}

/* performs merge sort and binary merge in the same thread */
void* tMergeSort(void * arg) {
    int threadIndex = (intptr_t) arg;
    int increment, listSize;
    int* p0 = x+threadIndex*nPerThread;
    mergeSort(p0, nPerThread);
    while (allDone == 0);
    int i;
    for(i=2; i<=P; i*=2) {
        increment = i/2; //increment for next thread to join
        listSize = increment * nPerThread; //list size to merge
        if (threadIndex%i==0){
            if(pthread_join(tPointer[threadIndex+increment], NULL)) {
                printf("threadIndex=%d. Error exiting thread %d.\n", threadIndex, threadIndex+increment);
                exit(0);
            }
            if (threadIndex+i == P) { //last thread may have >nPerThread numbers
                merge(p0, listSize, p0+listSize, N-(threadIndex+increment)*nPerThread);
            }
            else {
                merge(p0, listSize, p0+listSize, listSize);
            }
        }
        else break;
    }
}

/*for sorting last part of array if N is not divisible by P*/
void* tMergeSortLast(void *arg) {
    mergeSort(x+(P-1)*nPerThread, N-(P-1)*nPerThread);
}


/* merge sort of arg1 integers, starting at arg0 */

void mergeSort(int *arg0, int arg1) {
    int temp, count, *local, *ptr0, *ptr1, *ptr, count0, count1;

    if (arg1 == 1)
        return;
    else if (arg1 == 2) {
        if (*arg0 > *(arg0 + 1)) {
            temp = *arg0;
                    *arg0 = *(arg0 + 1);
                    *(arg0 + 1) = temp;
        }
        return;
    } else {
        count = arg1 / 2;
                ptr0 = arg0;
                ptr1 = arg0 + count;
                mergeSort(ptr0, count);
                mergeSort(ptr1, arg1 - count);
                merge(ptr0, count, ptr1, arg1 - count);

        return;
    }
}

/* merge two sorted lists, starting at p0 and p1 */
/* p0 has c0 elements, p1 has c1 elements */
/* copy resulting list to starting address p0 */

void merge(int *p0, int c0, int *p1, int c1) {
    int * local, *list, count, totCount;

            list = p0;
            local = (int *) calloc(c0 + c1, sizeof (int));
            count = c0 + c1;
            totCount = count;
    while (count > 0) {
        if (*p0 > *p1) {
            *local = *p1;
                    p1++;
                    c1--;
        } else {
            *local = *p0;
                    p0++;
                    c0--;
        }
        count--;
                local++;
        if ((c0 == 0) || (c1 == 0))
            break;
        }
    if (count != 0) {
        if (c0 == 0)
            while (c1 > 0) {
                *local = *p1;
                        p1++;
                        local++;
                        c1--;
            } else {
            while (c0 > 0) {
                *local = *p0;
                        p0++;
                        local++;
                        c0--;
            }
        }
    }

    local = local - totCount;

    for (count = 0; count < totCount; count++)
            *list++ = *local++;
    }

/* check if list is sorted */
void checkSort(int *data, int num) {
    int i;
    int flag = 0;
    for (i = 0; i < num - 1; i++) {
        if (data[i] > data[i + 1]) {
            flag = 1;
            break;
        }
    }
    if (flag == 0)
            printf("data is sorted\n");
    else
        printf("data is not sorted\n");
}
