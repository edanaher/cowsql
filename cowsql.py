from dataclasses import dataclass
from typing import Tuple, List
import time

SEGMENT_SIZE = 4

@dataclass
class Segment:
    id: str
    refCount: int
    rows: List[Tuple[int, str]]

@dataclass
class SegmentPointer:
    id: str
    size: int
    min: int
    max: int

def newSegmentId():
    return str(time.time())

def writeSegment(segment : Segment):
    with open(f"segments/{segment.id}", "w") as f:
        f.write(str(segment.refCount) + "\n")
        f.write("\n".join([f"{r[0]}\t{r[1]}" for r in segment.rows]))

def readSegment(id : str):
    with open(f"segments/{id}") as f:
        lines = f.readlines()
    refCount = int(lines[0])
    def readRow(str):
        id, name = str.strip().split("\t")
        return (int(id), name)
    rows = [readRow(line) for line in lines[1:]]
    return Segment(id, refCount, rows)

def createTable(name : str):
    with open(f"tables/{name}", "w") as f:
        f.close()

def readTable(name : str):
    with open(f"tables/{name}") as f:
        lines = f.readlines()

    t = []
    for l in lines:
        id, size, min, max = l.split("\t")
        t.append(SegmentPointer(id, int(size), int(min), int(max)))
    return t

def writeTable(name : str, table : List[SegmentPointer]):
    with open(f"tables/{name}", "w") as f:
        f.write("\n".join([f"{p.id}\t{p.size}\t{p.min}\t{p.max}" for p in table]))

def find_id(table : list[SegmentPointer], id : int):
    for p_index, p in enumerate(table):
        if id >= p.min and id <= p.max:
            segment = readSegment(p.id)
            index = next((i for i, r in enumerate(segment.rows) if r[0] == id), None)
            if index:
                return p_index, segment, index

    return (None, None, None)

def find_not_full(table : list[SegmentPointer]):
    available = (p_index for p_index, p in enumerate(table) if p.size < SEGMENT_SIZE)
    return next(available, None)

def upsert(name : str, id : int, value : str):
    table = readTable(name)
    p_index, segment, index = find_id(table, id)
    if segment:
        # The row exists; update it
        segment.rows[index] = (id, value)
        writeSegment(segment)
    else:
        p_index = find_not_full(table)
        if p_index != None:
            # Add to an existing non-full segment.
            segment = readSegment(table[p_index].id)
            segment.rows.append((id, value))
            writeSegment(segment)
            if id < table[p_index].min:
                table[p_index].min = id
            elif id > table[p_index].max:
                table[p_index].max = id
            table[p_index].size += 1
            writeTable(name, table)
        else:
            # Add a new segment
            segment = Segment(newSegmentId(), 1, [(id, value)])
            writeSegment(segment)
            table.append(SegmentPointer(segment.id, 1, id, id))
            writeTable(name, table)

    print(segment)

def query(name : str, id : int):
    table = readTable(name)
    _, segment, index = find_id(table, id)
    if not segment:
        return None
    return segment.rows[index][1]


def __main__():
    segment = Segment(newSegmentId(), 1, [(1, "Evan"), (2, "James")])
    writeSegment(segment)
    print(readSegment(segment.id))
    createTable("t1")
    upsert("t1", 1, "Evan")
    upsert("t1", 2, "Jim")
    upsert("t1", 2, "James")
    upsert("t1", 4, "Tim")
    upsert("t1", 5, "Nancy")
    upsert("t1", 3, "Binish")
    print(readTable("t1"))
    print(query("t1", 2))


__main__()
