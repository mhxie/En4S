/*
 * Copyright (c) 2019-2024, UC Santa Cruz
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  * Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <nvme/sw_counter.h>
#include <stdlib.h>

// Initialize the sliding window counter
void init_counter(struct sw_counter *counter, int K) {
    counter->byte_capacity =
        (K + 7) / 8;  // Calculate the number of bytes needed
    counter->buffer =
        (unsigned char *)calloc(counter->byte_capacity, sizeof(unsigned char));
    counter->head = 0;
    counter->tail = 0;
    counter->size = 0;
    counter->count_ones = 0;
    counter->count_zeros = 0;
    counter->capacity = K;
}

// Free the sliding window counter
void free_counter(struct sw_counter *counter) {
    free(counter->buffer);
    free(counter);
}

// Set a bit in the bit array
void set_bit(unsigned char *buffer, int index, int value) {
    int byte_index = index / 8;
    int bit_index = index % 8;
    if (value) {
        buffer[byte_index] |= (1 << bit_index);
    } else {
        buffer[byte_index] &= ~(1 << bit_index);
    }
}

// Get a bit from the bit array
int get_bit(unsigned char *buffer, int index) {
    int byte_index = index / 8;
    int bit_index = index % 8;
    return (buffer[byte_index] >> bit_index) & 1;
}

// Insert a value into the sliding window
void counter_insert(struct sw_counter *counter, int value) {
    // If the window is full, remove the oldest element
    if (counter->size == counter->capacity) {
        int oldest = get_bit(counter->buffer, counter->head);
        if (oldest == 1) {
            counter->count_ones--;
        } else {
            counter->count_zeros--;
        }
        counter->head = (counter->head + 1) % counter->capacity;
        counter->size--;
    }

    // Add the new value
    set_bit(counter->buffer, counter->tail, value);
    if (value == 1) {
        counter->count_ones++;
    } else {
        counter->count_zeros++;
    }
    counter->tail = (counter->tail + 1) % counter->capacity;
    counter->size++;
}

// Get the counts of 1s and 0s in the sliding window
void get_counts(struct sw_counter *counter, int *count_ones, int *count_zeros) {
    *count_ones = counter->count_ones;
    *count_zeros = counter->count_zeros;
}

// Get the ratio of 1s or 0s to the total elements in the sliding window
int get_ratio(struct sw_counter *counter, int value) {
    if (counter->size == 0) {
        return 0;  // To avoid division by zero
    }
    if (value == 1) {
        return (counter->count_ones * 100) / counter->size;
    } else {
        return (counter->count_zeros * 100) / counter->size;
    }
}
