from dataclasses import dataclass
from typing import List


@dataclass(frozen=False)
class StateValues:
    wdl_timestamp_10h: List
    dpst_timestamp_10h: List
    trsf_timestamp_10h: List
    sumry_timestamp_10h: List
    sumry_10h: List

    def __post_init__(self):
        if self.wdl_timestamp_10h is None:
            self.wdl_timestamp_10h = []
        if self.dpst_timestamp_10h is None:
            self.dpst_timestamp_10h = []
        if self.trsf_timestamp_10h is None:
            self.trsf_timestamp_10h = []
        if self.sumry_timestamp_10h is None:
            self.sumry_timestamp_10h = []
        if self.sumry_10h is None:
            self.sumry_10h = []


@dataclass(frozen=True)
class StreamValues:
    cstno: str
    evnt_typ: str
    evnt_dttm: int
    evnt_guid: str
    tx_amt: int
    acco_bal: int
    sumry: str