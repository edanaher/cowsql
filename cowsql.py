from dataclasses import dataclass
from typing import Tuple, List
import os
import time

SEGMENT_SIZE = 4

def new_segment_id():
    return str(time.time())

@dataclass
class Segment:
    id: str
    ref_count: int
    rows: List[Tuple[int, str]]

    def save(self):
        with open(f"segments/{self.id}", "w") as f:
            f.write(str(self.ref_count) + "\n")
            f.write("\n".join([f"{r[0]}\t{r[1]}" for r in self.rows]))

    def load(id : str):
        with open(f"segments/{id}") as f:
            lines = f.readlines()
        ref_count = int(lines[0])
        def read_row(str):
            id, name = str.strip().split("\t")
            return (int(id), name)
        rows = [read_row(line) for line in lines[1:]]
        return Segment(id, ref_count, rows)


@dataclass
class SegmentPointer:
    id: str
    size: int
    min: int
    max: int

@dataclass
class Table:
    name: str
    segments: List[SegmentPointer]

    def create(name: str):
        with open(f"tables/{name}", "w") as f:
            f.close()
        return Table(name, [])

    def load(name : str):
        with open(f"tables/{name}") as f:
            lines = f.readlines()

        segments = []
        for l in lines:
            id, size, min, max = l.split("\t")
            segments.append(SegmentPointer(id, int(size), int(min), int(max)))
        return Table(name, segments)

    def save(self):
        with open(f"tables/{self.name}", "w") as f:
            f.write("\n".join([f"{p.id}\t{p.size}\t{p.min}\t{p.max}" for p in self.segments]))

    def find_not_full(self):
        available = (p_index for p_index, p in enumerate(self.segments) if p.size < SEGMENT_SIZE)
        return next(available, None)

    def find_id(self, id : int):
        for p_index, p in enumerate(self.segments):
            if id >= p.min and id <= p.max:
                segment = Segment.load(p.id)
                index = next((i for i, r in enumerate(segment.rows) if r[0] == id), None)
                if index is not None:
                    return p_index, segment, index

        return (None, None, None)

def upsert(name : str, id : int, value : str):
    table = Table.load(name)
    p_index, segment, index = table.find_id(id)
    if segment:
        # The row exists; update it
        segment.rows[index] = (id, value)
        segment.save()
    else:
        p_index = table.find_not_full()
        if p_index != None:
            # Add to an existing non-full segment.
            segment = Segment.load(table.segments[p_index].id)
            segment.rows.append((id, value))
            segment.save()
            if id < table.segments[p_index].min:
                table.segments[p_index].min = id
            elif id > table.segments[p_index].max:
                table.segments[p_index].max = id
            table.segments[p_index].size += 1
            table.save()
        else:
            # Add a new segment
            segment = Segment(new_segment_id(), 1, [(id, value)])
            segment.save()
            table.segments.append(SegmentPointer(segment.id, 1, id, id))
            table.save()

    print(segment)

def query(name : str, id : int):
    table = Table.load(name)
    _, segment, index = table.find_id(id)
    if segment == None:
        return None
    return segment.rows[index][1]

def delete_segment(id : str):
    os.remove(f"segments/{id}")

def delete(name : str, id : int):
    table = Table.load(name)
    p_index, segment, index = table.find_id(id)
    print("deleting from", segment)
    if segment == None:
        return

    if table.segments[p_index].size == 1:
        delete_segment(segment.id)
        table.segments.pop(p_index)
        table.save()
    else:
        # TODO: we should update the min/max.
        segment.rows.pop(index)
        segment.save()
        table.segments[p_index].size -= 1
        table.save(table)


def __main__():
    Table.create("t1")
    upsert("t1", 1, "Evan")
    upsert("t1", 2, "Jim")
    upsert("t1", 2, "James")
    upsert("t1", 4, "Tim")
    upsert("t1", 5, "Nancy")
    upsert("t1", 3, "Binish")
    print(Table.load("t1"))
    print(query("t1", 2))
    print(query("t1", 1))
    delete("t1", 3)
    print(Table.load("t1"))
    print(query("t1", 3))


__main__()
