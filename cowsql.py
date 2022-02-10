from dataclasses import dataclass
from typing import Tuple, List
import time


@dataclass
class Segment:
    id: int
    refCount: int
    rows: List[Tuple[int, str]]

def newSegmentId():
    return str(time.time())

def writeSegment(segment : Segment):
    with open(f"segments/{segment.id}", "w") as f:
        f.write(str(segment.refCount) + "\n")
        f.write("\n".join([f"{r[0]}\t{r[1]}" for r in segment.rows]))


def __main__():
    segment = Segment(newSegmentId(), 1, [(1, "Evan"), (2, "James")])
    writeSegment(segment)

__main__()
