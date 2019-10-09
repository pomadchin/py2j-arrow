from py4j.java_gateway import JavaGateway
import pyarrow as pa

gateway = JavaGateway()
arr = gateway.entry_point.getTile().toArrowBytes()

reader = pa.ipc.open_stream(arr)
print(reader.schema)

batches = [b.to_pydict() for b in reader]

print(batches[0])