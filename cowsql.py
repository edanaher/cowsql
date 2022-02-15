"""Simple database
Demonstrates a super-simple file based id-value store.

Usage:
    cowsql.py create <table_name>
    cowsql.py query  <table_name> <id>
    cowsql.py upsert <table_name> <id> <value>
    cowsql.py delete <table_name> <id>
"""
from dataclasses import dataclass
from typing import Tuple, List
import os
import time

from docopt import docopt

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

    def delete(self):
        os.remove(f"segments/{self.id}")


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

    def upsert(self, id : int, value : str):
        p_index, segment, index = self.find_id(id)
        if segment:
            # The row exists; update it
            segment.rows[index] = (id, value)
            segment.save()
        else:
            p_index = self.find_not_full()
            if p_index != None:
                # Add to an existing non-full segment.
                segment = Segment.load(self.segments[p_index].id)
                segment.rows.append((id, value))
                segment.save()
                if id < self.segments[p_index].min:
                    self.segments[p_index].min = id
                elif id > self.segments[p_index].max:
                    self.segments[p_index].max = id
                self.segments[p_index].size += 1
                self.save()
            else:
                # Add a new segment
                segment = Segment(new_segment_id(), 1, [(id, value)])
                segment.save()
                self.segments.append(SegmentPointer(segment.id, 1, id, id))
                self.save()

    def query(self, id : int):
        _, segment, index = self.find_id(id)
        if segment == None:
            return None
        return segment.rows[index][1]

    def delete(self, id : int):
        p_index, segment, index = self.find_id(id)
        if segment == None:
            return

        if self.segments[p_index].size == 1:
            segment.delete()
            self.segments.pop(p_index)
            self.save()
        else:
            # TODO: we should update the min/max.
            segment.rows.pop(index)
            segment.save()
            self.segments[p_index].size -= 1
            self.save(table)

def main():
    opts = docopt(__doc__)
    if opts["create"]:
        Table.create(opts["<table_name>"])
    else:
        table = Table.load(opts["<table_name>"])
        id = int(opts["<id>"])
        value = opts["<value>"]
        if opts["query"]:
            print(table.query(id))
        elif opts["upsert"]:
            table.upsert(id, value)
        elif opts["delete"]:
            table.delete(id)
    print("Table is now", table)

main()
