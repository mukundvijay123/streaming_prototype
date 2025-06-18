import pyarrow as pa
import io
import struct
import os


def extractMetadata(event):
    tableMetadata = event.schema.metadata
    if tableMetadata:
        metadata_dict = {k.decode(): v.decode() for k, v in tableMetadata.items()}
        return metadata_dict
    else:
        return None 
    
def writerThreadTask(queue, topicInfoDict):
    while True:
        event = queue.get()
        eventMetadata = extractMetadata(event)
        topic = None
        if eventMetadata is not None:
            topic = eventMetadata["topic"]
            topicInfo = topicInfoDict[topic]
            with topicInfo.fileWriteLock:
                # Convert arrow stream to bytes
                sink = pa.BufferOutputStream()
                writer = pa.ipc.new_stream(sink, event.schema)
                writer.write(event)
                writer.close()
                serialized = sink.getvalue().to_pybytes()
                event_size = len(serialized)
                
                # Ensure values are within range for 'I' format (0 to 4294967295)
                if topicInfo.offset < 0 or topicInfo.offset > 4294967295:
                    print(f"Warning: Offset value {topicInfo.offset} out of range, resetting to 0")
                    topicInfo.offset = 0
                    
                if topicInfo.SegmentLogSize < 0 or topicInfo.SegmentLogSize > 4294967295:
                    print(f"Warning: SegmentLogSize {topicInfo.SegmentLogSize} out of range, resetting to 0")
                    topicInfo.SegmentLogSize = 0
                
                # Format: 4 bytes size followed by serialized data in log file
                size_bytes = struct.pack('>I', event_size)
                
                # Format: 4 bytes offset num, 4 bytes byte position in index file
                offset_bytes = struct.pack('>I', topicInfo.offset)
                byte_position_bytes = struct.pack('>I', topicInfo.SegmentLogSize)
                
                # Writing to log file (size + data)
                topicInfo.SegmentLog.write(size_bytes)
                topicInfo.SegmentLog.write(serialized)
                topicInfo.SegmentLog.flush()
                
                # Writing to index file (offset + position)
                topicInfo.SegmentIndex.write(offset_bytes)
                topicInfo.SegmentIndex.write(byte_position_bytes)
                topicInfo.SegmentIndex.flush()

                with topicInfo.updateObjectLock:
                    # Update position for next write
                    topicInfo.SegmentLogSize += (4 + event_size)  # Size bytes plus data
                    topicInfo.offset += 1 

def readThreadTask(partitionPath, topicInfoDict, topic, offset):
    partitionPath = os.path.join(partitionPath, topic)
    index_file_path = os.path.join(partitionPath, f"{0}.index")
    log_file_path = os.path.join(partitionPath, f"{0}.log")
    
    index_file = open(index_file_path, "rb")
    log_file = open(log_file_path, "rb")
    
    with topicInfoDict[topic].updateObjectLock:
        committed_offset = topicInfoDict[topic].offset
    
    # Adjust offset if too far behind
    if committed_offset - offset > 100:
        offset = committed_offset - 100
    
    # If not starting from beginning, seek to right position in index file
    if offset != 0:
        # Each index entry is 8 bytes (4 for offset, 4 for position)
        index_file.seek(8 * offset)
    
    while offset < committed_offset:
        # Read position from index file
        offset_bytes = index_file.read(4)
        if len(offset_bytes) != 4:
            break
            
        pos_bytes = index_file.read(4)
        if len(pos_bytes) != 4:
            break
            
        # Parse position
        log_position = struct.unpack('>I', pos_bytes)[0]
        
        # Seek to position in log file
        log_file.seek(log_position)
        
        # Read event size
        size_bytes = log_file.read(4)
        if len(size_bytes) != 4:
            break
            
        event_size = struct.unpack('>I', size_bytes)[0]
        
        # Read serialized event
        serialized = log_file.read(event_size)
        if len(serialized) != event_size:
            break
            
        # Convert back to Arrow table
        reader = pa.ipc.open_stream(pa.py_buffer(serialized))
        table = reader.read_all()
        
        yield table
        offset += 1
    
    # Clean up
    index_file.close()
    log_file.close()