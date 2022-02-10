from dataclasses import dataclass
from typing import Tuple, List
import time


@dataclass
class Segment:
    id: str
    refCount: int
    rows: List[Tuple[int, str]]

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
        rows = [tuple(line.strip().split("\t")) for line in lines[1:]]
        return Segment(id, refCount, rows)

def __main__():
    segment = Segment(newSegmentId(), 1, [(1, "Evan"), (2, "James")])
    writeSegment(segment)
    print(readSegment(segment.id))

__main__()
