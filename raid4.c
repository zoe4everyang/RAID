/*
 * RAID1 example for BUSE
 * by Tyler Bletsch to ECE566, Duke University, Fall 2019
 * 
 * Based on 'busexmp' by Adam Cozzette
 *
 * This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 */
 
#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <argp.h>
#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>

#include "buse.h"

#define UNUSED(x) (void)(x) // used to suppress "unused variable" warnings without turning off the feature entirely

int dev_total = 0;
int dev_fd[16] = {0}; // file descriptors for 3-16 underlying block devices that make up the RAID
int block_size;  //NOTE: other than truncating the resulting raid device, block_size is ignored in this program; it is asked for and set in order to make it easier to adapt this code to RAID0/4/5/6.
uint64_t raid_device_size; // size of raid device in bytes
bool verbose = false;  // set to true by -v option for debug output
bool degraded = false; // true if we're missing a device

int rebuild_dev = -1; // index of drive that is being added with '+' for RAID rebuilt
int degraded_dev = -1;
int parity_dev = -1;

static int xmp_read(void *buf, u_int32_t len, u_int64_t offset, void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "R - %lu, %u\n", offset, len);
    
    // if(offset < 0 || offset > raid_device_size || offset + len > raid_device_size) {
    //     perror("Read error: invalid offset or len");  //////// error needed???????
    //     return -1;
    // }
    // calculate which drive the data should be start from and to (based on offset and len)
    u_int64_t block_num_from = offset / block_size;
    u_int64_t block_num_to = (offset+len) / block_size;
    u_int64_t block_byte_to = block_size;
    u_int64_t bytes_read = 0;

    for(u_int64_t b = block_num_from; b <= block_num_to; b++){
        int dev_num = b % (dev_total - 1);
        u_int64_t dev_block_index = b / (dev_total - 1);
        u_int64_t block_offset = (offset + bytes_read) % block_size;
        u_int64_t dev_offset = dev_block_index * block_size + block_offset;
        ssize_t curr_bytes_read = -1;
        
        if(b == block_num_to) {
            block_byte_to = (offset+len) % block_size; 
        }

        // replace the degraded drive with:
        // XOR with surviving drive and all other drives
        if (dev_fd[dev_num] == -1){ // degraded drive
            int r;
            char temp_buf[block_byte_to - block_offset];
            char result_buf[block_byte_to - block_offset];
            memset(temp_buf, 0, block_byte_to - block_offset); 
            memset(result_buf, 0, block_byte_to - block_offset); 
            for(int i=0; i < dev_total; i++){
                if (i == dev_num) continue;
                r = pread(dev_fd[i], temp_buf, block_byte_to - block_offset, dev_offset);
                for(int j = 0; j < block_byte_to - block_offset; j++){
                    result_buf[j] ^= temp_buf[j];
                }
                if (r<0) {
                    perror("Read error");
                    return -1;
                } else if (r != block_byte_to - block_offset) {
                    fprintf(stderr, "read: short read (%d bytes)\n", r);
                    return 1;
                }
            }
            curr_bytes_read = block_byte_to - block_offset;
            memcpy(buf + bytes_read, result_buf, block_byte_to - block_offset);
        }else{ // normal drive
            curr_bytes_read = pread(dev_fd[dev_num], buf + bytes_read, block_byte_to - block_offset, dev_offset);
        }
        if (curr_bytes_read < 0){
            perror("Read error");
            return -1;
        }
        bytes_read += curr_bytes_read;
        if (bytes_read >= len) {
            break; 
        }
    }
    return 0;
}

static int xmp_write(const void *buf, u_int32_t len, u_int64_t offset, void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "W - %lu, %u\n", offset, len);
    
    // if(offset < 0 || offset > raid_device_size || offset + len > raid_device_size) {
    //     perror("Write error: invalid offset or len");  //// error needed?????
    //     return -1;
    // }
    u_int64_t block_num_from = offset / block_size;
    u_int64_t block_num_to = (offset+len) / block_size;
    u_int64_t block_byte_to = block_size;
    u_int64_t bytes_written = 0;

    for(u_int64_t b = block_num_from; b <= block_num_to; b++){
        int dev_num = b % (dev_total - 1);
        u_int64_t dev_block_index = b / (dev_total - 1);
        u_int64_t block_offset = (offset + bytes_written) % block_size;
        u_int64_t dev_offset = dev_block_index * block_size + block_offset;
        ssize_t curr_bytes_written = -1;
        
        if(b == block_num_to) {
            block_byte_to = (offset+len) % block_size; 
        }

        // replace the degraded drive with:
        // XOR all surviving drive with the writing content -> store in parity
        if (dev_fd[dev_num] == -1 && dev_num != dev_total - 1){ // degraded drive
            int r;
            char temp_buf[block_byte_to - block_offset];
            char result_buf[block_byte_to - block_offset];
            memset(temp_buf, 0, block_byte_to - block_offset);
            memset(result_buf, 0, block_byte_to - block_offset);
            for(int i=0; i < dev_total-1; i++){
                if (i == dev_num) continue;
                r = pread(dev_fd[i], temp_buf, block_byte_to - block_offset, dev_offset);
                for(int j = 0; j < block_byte_to - block_offset; j++){
                    result_buf[j] ^= temp_buf[j];
                }
                if (r<0) {
                    perror("Read error in write");
                    return -1;
                } else if (r != block_byte_to - block_offset) {
                    fprintf(stderr, "Read error in write: short read (%d bytes)\n", r);
                    return 1;
                }
            }
            // XOR with the data to be written in degraded drive -> write to parity directly
            memcpy(temp_buf, buf + bytes_written, block_byte_to - block_offset);
            for (int i = block_offset; i < block_byte_to; i++){
                result_buf[i] ^= temp_buf[i];
            }
            curr_bytes_written = pwrite(dev_fd[parity_dev], result_buf, block_byte_to - block_offset, dev_offset);
        }else{ // normal drive
            char old_b[block_byte_to - block_offset];
            char old_p[block_byte_to - block_offset];
            int rb = pread(dev_fd[dev_num], old_b, block_byte_to - block_offset, dev_offset);
            if (rb < 0){
                perror("Read error");
                return -1;
            }
            int rp = pread(dev_fd[parity_dev], old_p, block_byte_to - block_offset, dev_offset);
            if (rp < 0){
                perror("Read error");
                return -1;
            }
            curr_bytes_written = pwrite(dev_fd[dev_num], buf + bytes_written, block_byte_to - block_offset, dev_offset);
            if (curr_bytes_written < 0){
                perror("Write error");
                return -1;
            }
            // write to parity (using single small write)
            if (dev_fd[dev_total - 1] != -1){
                char new_p[block_byte_to - block_offset];
                char new_b[block_byte_to - block_offset];
                memcpy(new_b, buf + bytes_written, block_byte_to - block_offset);
                for(int i = block_offset; i < block_byte_to; i++){
                    new_p[i] = old_b[i] ^ old_p[i] ^ new_b[i];
                }
                ssize_t parity_bytes_written = pwrite(dev_fd[parity_dev], new_p, block_byte_to - block_offset, dev_offset);
                if (parity_bytes_written < 0){
                    perror("Write error");
                    return -1;
                }
            }   
        }
        bytes_written += curr_bytes_written;
        if (bytes_written >= len) {
            break; 
        }
    }
    return 0;
}

static int xmp_flush(void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "Received a flush request.\n");
    for (int i=0; i<dev_total; i++) {
        if (dev_fd[i] != -1) { // handle degraded mode
            fsync(dev_fd[i]); // we use fsync to flush OS buffers to underlying devices
        }
    }
    return 0;
}

static void xmp_disc(void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "Received a disconnect request.\n");
    // disconnect is a no-op for us
}

/*
// we'll disable trim support, you can add it back if you want it
static int xmp_trim(u_int64_t from, u_int32_t len, void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "T - %lu, %u\n", from, len);
    // trim is a no-op for us
    return 0;
}
*/

/* argument parsing using argp */

static struct argp_option options[] = {
    {"verbose", 'v', 0, 0, "Produce verbose output", 0},
    {0},
};

struct arguments {
    uint32_t block_size;
    char* device[16];
    char* raid_device;
    int verbose;
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    struct arguments *arguments = state->input;
    char * endptr;

    switch (key) {

        case 'v':
            arguments->verbose = 1;
            break;

        case ARGP_KEY_ARG: // parsing arguments after options
            if (state->arg_num == 0){
                arguments->block_size = strtoul(arg, &endptr, 10);
                if (*endptr != '\0') {
                    /* failed to parse integer */
                    errx(EXIT_FAILURE, "SIZE must be an integer");
                }
                break;
            }
            else if (state->arg_num == 1){
                arguments->raid_device = arg;
                break;
            }
            else if (state->arg_num > 1 && state->arg_num < 18){
                dev_total++;
                arguments->device[state->arg_num - 2] = arg;
                break;
            }
            else{
                warnx("too many arguments (valid number of drives are 3 to 16)");
                    /* Too many arguments. */
                return ARGP_ERR_UNKNOWN;
            }
            break;

        case ARGP_KEY_END:
            //dev_total = state->arg_num - 2;
            if (state->arg_num < 4) {
                warnx("not enough arguments");
                argp_usage(state);
            }
            break;

        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static struct argp argp = {
    .options = options,
    .parser = parse_opt,
    .args_doc = "BLOCKSIZE RAIDDEVICE DEVICE1 DEVICE2 DEVICE3 ... (up to 16 DEVICEs)",
    .doc = "BUSE implementation of RAID1 for two devices.\n"
           "`BLOCKSIZE` is an integer number of bytes. "
           "\n\n"
           "`RAIDDEVICE` is a path to an NBD block device, for example \"/dev/nbd0\"."
           "\n\n"
           "`DEVICE*` is a path to underlying block devices. Normal files can be used too. A `DEVICE` may be specified as \"MISSING\" to run in degraded mode. "
           "(Only one device can be MISSING otherwise RAID4 cannot be built)"
           "\n\n"
           "If you prepend '+' to a DEVICE, you are re-adding it as a replacement to the RAID, and we will rebuild the array. "
           "This is synchronous; the rebuild will have to finish before the RAID is started. "
};

static int do_raid_rebuild() {
    // target drive index is: rebuild_dev
    char buf[block_size]; 
    char result_buf[block_size];
    memset(buf, 0, block_size); 

    //lseek(dev_fd[source_dev],0,SEEK_SET);
    //lseek(dev_fd[rebuild_dev],0,SEEK_SET);
    
    // simple block copy
    for (uint64_t cursor=0; cursor<raid_device_size; cursor+=block_size) {
        int r;
        memset(result_buf, 0, block_size); 
        for(int i=0; i < dev_total; i++){
            if (i == rebuild_dev) continue;
            r = pread(dev_fd[i], buf, block_size, cursor);
            for(int j = 0; j < block_size; j++){
                result_buf[j] ^= buf[j];
            }
            if (r<0) {
                perror("rebuild_read");
                return -1;
            } else if (r != block_size) {
                fprintf(stderr, "rebuild_read: short read (%d bytes), offset=%zu\n", r, cursor);
                return 1;
            }
        }
        r = pwrite(dev_fd[rebuild_dev],result_buf,block_size,cursor);
        if (r<0) {
            perror("rebuild_write");
            return -1;
        } else if (r != block_size) {
            fprintf(stderr, "rebuild_write: short write (%d bytes), offset=%zu\n", r, cursor);
            return 1;
        }
    }
    degraded_dev = -1;
    rebuild_dev = -1;
    return 0;
}

int main(int argc, char *argv[]) {
    struct arguments arguments = {
        .verbose = 0,
    };
    argp_parse(&argp, argc, argv, 0, 0, &arguments);
    
    struct buse_operations bop = {
        .read = xmp_read,
        .write = xmp_write,
        .disc = xmp_disc,
        .flush = xmp_flush,
        // .trim = xmp_trim, // we'll disable trim support, you can add it back if you want it
    };

    verbose = arguments.verbose;
    block_size = arguments.block_size;
    raid_device_size=0; // will be detected from the drives available
    bool rebuild_needed = false; // will be set to true if a drive is MISSING
    printf("device count: %d\n", dev_total);
    for (int i=0; i < dev_total; i++) {
        char* dev_path = arguments.device[i];
        if (strcmp(dev_path,"MISSING")==0) {
            if(degraded){
                fprintf(stderr, "ERROR: Multiple 'MISSING' drives specified. Can only handle one degraded drive at a time.\n");
                exit(1);
            }
            degraded = true;
            dev_fd[i] = -1;
            degraded_dev = i;
            fprintf(stderr, "DEGRADED: Device number %d is missing!\n", i);
        } else {
            if (dev_path[0] == '+') { // RAID rebuild mode!!
                if (rebuild_needed) {
                    // multiple +drives detected
                    fprintf(stderr, "ERROR: Multiple '+' drives specified. Can only recover one drive at a time.\n");
                    exit(1);
                }
                dev_path++; // shave off the '+' for the subsequent logic
                rebuild_dev = i;
                rebuild_needed = true;
            }
            dev_fd[i] = open(dev_path,O_RDWR);
            if (dev_fd[i] < 0) {
                perror(dev_path);
                exit(1);
            }
            uint64_t size = lseek(dev_fd[i],0,SEEK_END); // used to find device size by seeking to end
            fprintf(stderr, "Got device '%s', size %ld bytes.\n", dev_path, size);
            if (raid_device_size==0 || size<raid_device_size) {
                raid_device_size = size; // raid_device_size is minimum size of available devices
            }
        }

    }
    parity_dev = dev_total - 1;
    if (dev_fd[parity_dev] != -1){
        fprintf(stderr, "Assigning '%s' as parity.\n", arguments.device[parity_dev]);
    }else{
        fprintf(stderr, "Parity is missing.\n");
    }

    raid_device_size = raid_device_size/block_size*block_size; // divide+mult to truncate to block size
    bop.size = raid_device_size * (dev_total - 1); // tell BUSE how big our block device is
    bop.blksize = arguments.block_size;
    if (rebuild_needed) {
        if (degraded) {
            fprintf(stderr, "ERROR: Can't rebuild from a missing device (i.e., you can't combine MISSING and '+').\n");
            exit(1);
        }
        fprintf(stderr, "Doing RAID rebuild...\n");
        if (do_raid_rebuild() != 0) { 
            // error on rebuild
            fprintf(stderr, "Rebuild failed, aborting.\n");
            exit(1);
        }
    }
    fprintf(stderr, "RAID device resulting size: %ld.\nRAID device: %s\n", bop.size, arguments.raid_device);
    
    return buse_main(arguments.raid_device, &bop, NULL);
}
