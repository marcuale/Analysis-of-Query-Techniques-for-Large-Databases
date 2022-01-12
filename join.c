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

#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <omp.h>

#include "hash-nolock.c"
#include "join.h"

// SELECT count(*) FROM Student s, TA t
// WHERE s.sid = t.sid AND s.gpa > 3.0;

void data_partition_helper(const student_record *students, int s_start, int s_end, const ta_record *tas, int *ta_start, int *ta_end, int tas_count){
	for(int i = 0; i < tas_count; i++){
		
		if (tas[i].sid > students[s_end-1].sid) break;
		
		if (tas[i].sid >= students[s_start].sid && tas[i].sid <= students[s_end-1].sid){
			if (*ta_start == -1){
				*ta_start = i;
				*ta_end = i;
			} else {
				*ta_end = *ta_end + 1;
			}
		}
	}
}

int join_nested(const student_record *students, int students_count, const ta_record *tas, int tas_count)
{
	assert(students != NULL);
	assert(tas != NULL);

	int result = 0;
	int i, j;
	for (i=0; i<students_count; i++) {
		// if does not meet the requirement
		if (students[i].gpa <= 3.0) {
			continue;
		}
		// if meet the requirement, continue, execution.
		for (j=0; j<tas_count; j++) {
			if (tas[j].sid == students[i].sid) {
				result++;
			}
		}
	}

	return result;
}



// Assumes that records in both tables are already sorted by sid
int join_merge(const student_record *students, int students_count, const ta_record *tas, int tas_count)
{
	assert(students != NULL);
	assert(tas != NULL);

	int result = 0;
	int i = 0;
	int j = 0;
	int target_id;

	while ((i<students_count) && (j<tas_count)) {
		if (students[i].sid > tas[j].sid) {
			j++;
		}
		else if (students[i].sid < tas[j].sid){
			i++;
		}
		else
		{
			if (students[i].gpa <= 3.0) {
				i++;
				j++;
				continue;
			}

			// match and meet the requirement
			result++;
			target_id = students[i].sid;

			i++;
			while ((i<students_count) && (students[i].sid == target_id))
			{
				result++;
				i++;
			}

			j++;
			while ((j<tas_count) && (tas[j].sid == target_id))
			{
				result++;
				j++;
			}
		}
	}

	return result;
}


int join_hash(const student_record *students, int students_count, const ta_record *tas, int tas_count)
{
	assert(students != NULL);
	assert(tas != NULL);

	int result = 0;
	int i, j;

	hash_table_t *hash_table = hash_create(next_prime(students_count));

	for (i=0; i<students_count; i++) {
		hash_put(hash_table, students[i].sid, students[i].gpa, 0);
	}

	for (j=0; j<tas_count; j++) {
		if (hash_get(hash_table, tas[j].sid, 0) > 3.0){
			result++;
		}
	}

	return result;
}

int join_hash_symmetric(const student_record *students, int students_count, const ta_record *tas, int tas_count, hash_table_t *hash_table)
{
	assert(students != NULL);
	assert(tas != NULL);

	int result = 0;
	int i, j;

	for (i=0; i<students_count; i++) {
		hash_put(hash_table, students[i].sid, students[i].gpa, 1);
	}

	for (j=0; j<tas_count; j++) {
		if (hash_get(hash_table, tas[j].sid, 1) > 3.0){
			result++;
		}
	}

	return result;
}

