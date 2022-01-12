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
#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#include "hash.h"

// idea from:
// https://stackoverflow.com/questions/31930046/what-is-a-hash-table-and-how-do-you-make-it-in-c

struct hash_element {
	int key;
	double value;
	struct hash_element* next;
} typedef hash_element;

struct _hash_table_t {
	int size;// should be a prime number
	hash_element** elements;
};



static bool is_prime(int n)
{
	assert(n > 0);
	for (int i = 2; i <= sqrt(n); i++) {
		if (n % i == 0) return false;
	}
	return true;
}

// Get the smallest prime number that is not less than n (for hash table size computation)
int next_prime(int n)
{
	for (int i = n; ; i++) {
		if (is_prime(i)) return i;
	}
	assert(false);
	return 0;
}


// Create a hash table with 'size' buckets; the storage is allocated dynamically using malloc(); returns NULL on error
hash_table_t *hash_create(int size)
{
	assert(size > 0);
	hash_table_t* table = malloc(sizeof(hash_table_t));
	table->size = size;
	table->elements = malloc(size*sizeof(hash_element*));
	int i;
	for (i =0; i<size; i++){
		table->elements[i] = malloc(sizeof(hash_element));
		table->elements[i]->key = -1;
		table->elements[i]->value = -1;
		table->elements[i]->next = NULL;
	}

	return table;
}

// Release all memory used by the hash table, its buckets and entries
void hash_destroy(hash_table_t *table)
{
	assert(table != NULL);

	int i;
	for (i=0; i<table->size; i++){
		// no stored element
		if (table->elements[i]->key<0){
			free(table->elements[i]);
		}
		// there are store element
		hash_element* cur = table->elements[i];
		hash_element* container = table->elements[i];
		while(cur!=NULL){
			container = cur->next;
			free(cur);
			cur = container;
		}
	}
	
	// free the tree
	free(table->elements);
	free(table);

}


// Returns -1 if key is not found
double hash_get(hash_table_t *table, int key, int function)
{
	assert(table != NULL);

	int index;
	if(function == 0) {
		index = key % table->size;
	} else {
		index = key;
	}

	// seach for key
	hash_element *cur = table->elements[index];

	// no element stored
	if (cur->key<0){
		return -1;
	}
	while(cur != NULL){
		if(cur->key == key){
			return cur->value;
		}
		cur = cur->next;
	}

	// key not found
	return -1;
}

// Returns 0 on success, -1 on failure
int hash_put(hash_table_t *table, int key, double value, int function)
{
	assert(table != NULL);
	int index;
	if(function == 0) {
		index = key % table->size;
	} else {
		index = key;
	}
	// seach for key
	hash_element *cur = table->elements[index];
	// no stroage, first element
	if (cur->key < 0){
		cur->key = key;
		cur->value = value;
		cur->next = NULL;
		return 0;
	}

	// already exist
	hash_element *prev = NULL;
	while(cur != NULL){
		if(cur->key == key){
			cur->value = value;
			return 0;
		}
		prev = cur;
		cur = cur->next;
	}

	// new element to be append
	if ((cur == NULL) && (prev != NULL)){
		hash_element *new_element = malloc(sizeof(hash_element));
		prev->next = new_element;
		new_element->key = key;
		new_element->value = value;
		new_element->next = NULL;
		return 0;
	}

	return 1;
	
}
