// ------------
// This code is provided solely for the personal and private use of
// students taking the CSC367 course at the University of Toronto.
// Copying for purposes other than this use is expressly prohibited.
// All forms of distribution of this code, whether as given or with
// any changes, are expressly prohibited.
//
// Authors: Bogdan Simion, Maryam Dehnavi, Alexey Khrabrov
//
// All of the files in this directory and all subdirectories are:
// Copyright (c) 2020 Bogdan Simion and Maryam Dehnavi
// -------------

#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#include "join.h"
#include "options.h"


int main(int argc, char *argv[])
{
	const char *path = parse_args(argc, argv);
	if (path == NULL) return 1;

	if (!opt_replicate && !opt_symmetric) {
		fprintf(stderr, "Invalid arguments: parallel algorithm (\'-r\' or \'-s\') is not specified\n");
		print_usage(argv);
		return 1;
	}

	if (opt_nthreads <= 0) {
		fprintf(stderr, "Invalid arguments: number of threads (\'-t\') not specified\n");
		print_usage(argv);
		return 1;
	}
	omp_set_num_threads(opt_nthreads);

	int students_count, tas_count;
	student_record *students;
	ta_record *tas;
	if (load_data(path, &students, &students_count, &tas, &tas_count) != 0) return 1;
	join_func_t *join_f = opt_nested ? join_nested : (opt_merge ? join_merge : join_hash);
	
	
	double t_start = omp_get_wtime();				// Entry point
	
	int result = 0;
	int count = -1;
	// Fragment-and-replicate
	hash_table_t *hash_table = hash_create(students_count);
	
	if(opt_replicate){
		// more student, partitate studnet
		if (students_count > tas_count){
			int avg_work = students_count/opt_nthreads;
			#pragma omp parallel default(none) shared(avg_work, tas, tas_count,students, students_count, opt_nthreads, join_f) reduction(+:result) num_threads(opt_nthreads)
			{
				int cur_id = omp_get_thread_num();
				int cur_work = avg_work;
				// if the last, take the rest
				if (cur_id == (opt_nthreads-1)){
					cur_work += students_count%opt_nthreads;
				}
				result += join_f(&students[cur_id*avg_work], cur_work, tas, tas_count);
			}
		} 
		// more ta, partitate ta
		else {
			int avg_work = tas_count/opt_nthreads;
			#pragma omp parallel default(none) shared(avg_work, tas, tas_count,students, students_count, opt_nthreads, join_f) reduction(+:result) num_threads(opt_nthreads)
			{
				int cur_id = omp_get_thread_num();
				int cur_work = avg_work;
				// if the last, take the rest
				if (cur_id == (opt_nthreads-1)){
					cur_work += tas_count%opt_nthreads;
				}
				result += join_f(students, students_count, &tas[cur_id*avg_work], cur_work);
			}
		}
	}
	
	// Symmetric partitioning
	if(opt_symmetric){
		//hash_table_t *hash_table = hash_create(students_count);		// Create hash table if using join_hash
		#pragma omp parallel shared(students, tas, students_count, tas_count, hash_table, opt_merge, opt_nested) reduction(+:result)
		{
			int thread_id = omp_get_thread_num();
			int num_threads = omp_get_num_threads();
			int partition_size = students_count / num_threads;
			int cur_work = partition_size;
			int start_index = thread_id * partition_size;
			int end_index = (thread_id + 1 == num_threads) ? students_count : (thread_id + 1) * partition_size;
			int ta_start_index = -1;
			int ta_end_index = -1;
			int my_result = 0;
			
			// partition data
			data_partition_helper(students, start_index, end_index, tas, &ta_start_index, &ta_end_index, tas_count);

			if (thread_id == (opt_nthreads-1)){
				cur_work += students_count % opt_nthreads;
			}
			
			if (ta_start_index != -1) {
				if(!opt_merge && !opt_nested){					// Proceed with hash table method
					my_result = join_hash_symmetric(students + start_index, cur_work, tas + ta_start_index, (ta_end_index+1)-ta_start_index, hash_table);
				} else {						// Other Join Methods
					my_result = join_f(students + start_index, cur_work, tas + ta_start_index, (ta_end_index+1)-ta_start_index);
				}
			}
			result += my_result;
			
		}
	}

	//TODO: parallel join using OpenMP
	count = result;

	double t_end = omp_get_wtime();
	if (count < 0) goto end;
	printf("%d\n", count);
	printf("%f\n", (t_end - t_start) * 1000.0);
	result = 0;

end:
	free(students);
	free(tas);
	return result;
}
