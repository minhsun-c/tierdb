#pragma once

/**
 * container_of - retrieve the enclosing struct from a member pointer
 * @ptr:    pointer to the member
 * @type:   type of the enclosing struct
 * @member: name of the member field within the struct
 */
#define container_of(ptr, type, member) \
    ((type *) ((char *) (ptr) - offsetof(type, member)))