# cython: language_level=3
# protocol.pxd
#
# protocol header

# cimport cpython
# from cpython cimport Py_buffer

include "reflex.pxd"


# cdef class ReFlexBuffer:
#     cdef unsigned nbytes

#     def __cinit__(self):
#         pass

#     def __get_buffer__(self, Py_buffer *buffer, int flags):
#         buffer.obj = self

#         self.view_count += 1

#     def __releasebuffer__(self, Py_buffer *buffer):
#         self.view_count -=1
