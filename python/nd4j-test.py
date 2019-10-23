from py4j.java_gateway import JavaGateway
import numpy as np
import pyarrow as pa

class Nd4jIdentityType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, pa.uint32(), "nd4j-identity")

    def __arrow_ext_serialize__(self):
        return b''

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        return Nd4jIdentityType()

def ipc_write_batch(batch):
    stream = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return stream.getvalue()

gateway = JavaGateway()
pa.register_extension_type(Nd4jIdentityType())

nd4j_type = Nd4jIdentityType()
arr = pa.ExtensionArray.from_storage(nd4j_type, pa.array([4, 40], pa.uint32()))
batch = pa.RecordBatch.from_arrays([arr], ["eyes"])
buf = ipc_write_batch(batch)
gateway.entry_point.send_batch(buf.to_pybytes())

eye_recv = gateway.entry_point.identity(4)
print("received buffer of size", len(eye_recv))

bos = pa.BufferOutputStream()
tens = pa.Tensor.from_numpy(np.eye(4))
pa.ipc.write_tensor(tens, bos)
bos.close()
byts = bos.getvalue().to_pybytes()
gateway.entry_point.send(byts)
