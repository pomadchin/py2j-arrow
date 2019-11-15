from py4j.java_gateway import JavaGateway
import pyarrow as pa

gateway = JavaGateway()
arr = gateway.entry_point.getTile().toArrowBytes()

# https://arrow.apache.org/docs/python/ipc.html#using-streams
# TODO: Check that open_stream makes no allocations!!
# An important point is that if the input source supports zero-copy reads
# (e.g. like a memory map, or pyarrow.BufferReader),
# then the returned batches are also zero-copy and do not allocate any new memory on read.
reader = pa.ipc.open_stream(arr)
print(reader.schema)

batches = [b for b in reader]

pandas_arr = batches[0].to_pandas(zero_copy_only=True)
py_arr = batches[0].to_pydict()

# What are Tensors? https://github.com/apache/arrow/blob/74b9294bf63ff49818f2d6a72877139a1a540f60/python/pyarrow/tensor.pxi#L53 zero cost
# https://github.com/apache/arrow/blob/c805b5fadb548925c915e0e130d6ed03c95d1398/python/pyarrow/array.pxi#L934 # it is a numpy view
column = batches[0].column(0)
numpy_arr = column.to_numpy()

print("---------------")
print(type(column)) # pyarrow.lib.Int32Array
print("----pandas-----")
print(type(pandas_arr)) # pandas.core.frame.DataFrame
print(pandas_arr)
print("-------py------")
print(type(py_arr)) # dict
print(py_arr)
print("------numpy----")
print(type(numpy_arr)) # numpy.ndarray
print(numpy_arr)
