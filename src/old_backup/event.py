from typing import Dict

from pyflink.common import Time, Types
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from src.dataclass import StateValues, StreamValues

import json

class PreprocessEvent(KeyedProcessFunction):
    def __init__(self):
        self.withdrawal_timestamp_state = None
        self.deposit_timestamp_state = None
        self.transfer_timestamp_state = None
        self.summary_timestamp_state = None
        self.summary_state = None
        self.withdrawal_timestamp_descriptor = None
        self.deposit_timestamp_descriptor = None
        self.transfer_timestamp_descriptor = None
        self.summary_timestamp_descriptor = None
        self.summary_descriptor = None

    def create_value_state_descriptor(self):
        # Descriptor
        self.withdrawal_timestamp_descriptor = ValueStateDescriptor(
            "wdl_timestamp_10h", Types.LIST(Types.STRING())
        )
        self.deposit_timestamp_descriptor = ValueStateDescriptor(
            "dpst_timestamp_10h", Types.LIST(Types.STRING())
        )
        self.transfer_timestamp_descriptor = ValueStateDescriptor(
            "trsf_timestamp_10h", Types.LIST(Types.STRING())
        )
        self.summary_timestamp_descriptor = ValueStateDescriptor(
            "sumry_timestamp_10h", Types.LIST(Types.STRING())
        )
        self.summary_descriptor = ValueStateDescriptor(
            "sumry_10h", Types.LIST(Types.STRING())
        )

        # ttl
        ttl_10_hours_config = self._get_ttl_config(time=Time.seconds(10))
        self.withdrawal_timestamp_descriptor.enable_time_to_live(ttl_10_hours_config)
        self.deposit_timestamp_descriptor.enable_time_to_live(ttl_10_hours_config)
        self.transfer_timestamp_descriptor.enable_time_to_live(ttl_10_hours_config)
        self.summary_timestamp_descriptor.enable_time_to_live(ttl_10_hours_config)
        self.summary_descriptor.enable_time_to_live(ttl_10_hours_config)

    @staticmethod
    def _get_ttl_config(time: Time) -> StateTtlConfig:
        ttl_config = (
            StateTtlConfig.new_builder(time)
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        return ttl_config

    def open(self, runtime_context):
        self.create_value_state_descriptor()
        self.withdrawal_timestamp_state = runtime_context.get_state(
            self.withdrawal_timestamp_descriptor
        )
        self.deposit_timestamp_state = runtime_context.get_state(
            self.deposit_timestamp_descriptor
        )
        self.transfer_timestamp_state = runtime_context.get_state(
            self.transfer_timestamp_descriptor
        )
        self.summary_timestamp_state = runtime_context.get_state(
            self.summary_timestamp_descriptor
        )
        self.summary_state = runtime_context.get_state(self.summary_descriptor)

    def process_element(self, value, ctx):
        
        
        # print(value)

        key = ctx.get_current_key()  # acno
        print("coin :", key)

        print("value :",value)
        # # Event
        price = eval(json.loads(value))['Symbol']
        
        print(type(price))
        print(price)
        # event_context = json.loads(event["eventContext"])

        # # States
        # stream_states = self._get_stream_states()
        # state_values = self._get_state_values(
        #     evnt_typ=event["eventType"],
        #     evnt_dttm=event["eventTimestamp"],
        #     evnt_guid=event_context["guid"],
        #     event_context=event_context,
        # )

        # self._set_stream_states(states=stream_states, values=state_values)
        # feature = self._generate_feature(
        #     key=key, states=stream_states, values=state_values
        # )

        # print("state_len ::", len(self.deposit_timestamp_state.value()))
        
        if self.deposit_timestamp_state.value() is None:
            a = []
        else:
            a = self.deposit_timestamp_state.value()
        a.append(value)
        
        self.deposit_timestamp_state.update(a)
        
        # print(self.deposit_timestamp_state.value())
        
        # if len(self.deposit_timestamp_state.value()) > 100:
        #     self.deposit_timestamp_state.clear()
        #     self.withdrawal_timestamp_state.clear()
        #     self.transfer_timestamp_state.clear()
        #     self.summary_timestamp_state.clear()
        #     self.summary_state.clear()

        # yield self.event_messages.get_event(feature)
        yield str({"BTC_PRICE": price})

    # def _generate_feature(
    #     self, key: int, states: StreamStates, values: StateValues
    # ) -> Dict:
    #     # sumry_emb = self.vectorizer.vectorize(states.sumry_10h + [values.sumry]).tolist()  # feature

    #     feature = {
    #         "acno": str(int(key)),
    #         "cstno": str(int(values.cstno)),
    #         "evnt_guid": values.evnt_guid,
    #         "wdl_ts": ",".join(states.wdl_timestamp_10h),
    #         "mnrc_ts": ",".join(states.dpst_timestamp_10h),
    #         "trsf_ts": ",".join(states.trsf_timestamp_10h),
    #         "sumry_raw": ",".join(states.sumry_10h),
    #     }

    #     return feature

    # def _get_stream_states(self) -> StreamStates:
    #     return StreamStates(
    #         wdl_timestamp_10h=self.withdrawal_timestamp_state.value(),
    #         dpst_timestamp_10h=self.deposit_timestamp_state.value(),
    #         trsf_timestamp_10h=self.transfer_timestamp_state.value(),
    #         sumry_timestamp_10h=self.summary_timestamp_state.value(),
    #         sumry_10h=self.summary_state.value(),
    #     )

    # @staticmethod
    # def _get_state_values(
    #     evnt_typ: str, evnt_dttm: int, evnt_guid: str, event_context
    # ) -> StateValues:
    #     cstno = event_context["customerNumber"]
    #     tx_amt = event_context["transactionAmount"]
    #     acco_bal = event_context["customerAccountBalance"]
    #     if evnt_typ in ("DEPOSIT", "WITHDRAWAL"):
    #         sumry = event_context["summary"]
    #     else:
    #         sumry = event_context["beneficiaryAccountHolder"]
    #     if len(sumry) == 3:
    #         sumry = "개인"
    #     return StateValues(
    #         cstno=cstno,
    #         evnt_typ=evnt_typ,
    #         evnt_dttm=evnt_dttm,
    #         evnt_guid=evnt_guid,
    #         tx_amt=tx_amt,
    #         acco_bal=acco_bal,
    #         sumry=sumry,
    #     )

    # def _set_stream_states(self, states: StreamStates, values: StateValues) -> None:
    #     stream_states = self._update_stream_states(states, values)

    #     self.withdrawal_timestamp_state.update(stream_states.wdl_timestamp_10h)
    #     self.deposit_timestamp_state.update(stream_states.dpst_timestamp_10h)
    #     self.transfer_timestamp_state.update(stream_states.trsf_timestamp_10h)
    #     self.summary_timestamp_state.update(stream_states.sumry_timestamp_10h)
    #     self.summary_state.update(stream_states.sumry_10h)

    # @staticmethod
    # def _get_cut_index(ts, current_dttm):
    #     for i in range(len(ts)):
    #         if int(current_dttm) - int(ts[i].split("|")[0]) <= 1000 * 60 * 60 * 10:
    #             return i

    #     return len(ts)

    # def _update_stream_states(
    #     self, states: StreamStates, values: StateValues
    # ) -> StreamStates:
    #     if values.evnt_typ == "WITHDRAWAL":
    #         states.wdl_timestamp_10h.append(str(values.evnt_dttm))
    #     elif values.evnt_typ == "DEPOSIT":
    #         states.dpst_timestamp_10h.append(str(values.evnt_dttm))
    #     elif values.evnt_typ == "TRANSFER":
    #         states.trsf_timestamp_10h.append(f"{values.evnt_dttm}|{values.tx_amt}")

    #     states.sumry_timestamp_10h.append(str(values.evnt_dttm))

    #     sumry = values.sumry
    #     for repl in ["|", ","]:
    #         sumry = sumry.replace(repl, "")

    #     states.sumry_10h.append(sumry)

    #     states.wdl_timestamp_10h = np.array(states.wdl_timestamp_10h)[
    #         np.argsort(np.array(states.wdl_timestamp_10h, dtype=np.int64))
    #     ].tolist()
    #     states.dpst_timestamp_10h = np.array(states.dpst_timestamp_10h)[
    #         np.argsort(np.array(states.dpst_timestamp_10h, dtype=np.int64))
    #     ].tolist()
    #     states.trsf_timestamp_10h = np.array(states.trsf_timestamp_10h)[
    #         np.argsort([int(ts.split("|")[0]) for ts in states.trsf_timestamp_10h])
    #     ].tolist()

    #     sumry_new_idx = np.argsort(np.array(states.sumry_timestamp_10h, dtype=np.int64))
    #     states.sumry_10h = np.array(states.sumry_10h)[sumry_new_idx].tolist()
    #     states.sumry_timestamp_10h = np.array(states.sumry_timestamp_10h)[
    #         sumry_new_idx
    #     ].tolist()

    #     wdl_cut_index = self._get_cut_index(states.wdl_timestamp_10h, values.evnt_dttm)
    #     dpst_cut_index = self._get_cut_index(
    #         states.dpst_timestamp_10h, values.evnt_dttm
    #     )
    #     trsf_cut_index = self._get_cut_index(
    #         states.trsf_timestamp_10h, values.evnt_dttm
    #     )
    #     sumry_cut_index = self._get_cut_index(
    #         states.sumry_timestamp_10h, values.evnt_dttm
    #     )

    #     states.wdl_timestamp_10h = states.wdl_timestamp_10h[wdl_cut_index:]
    #     states.dpst_timestamp_10h = states.dpst_timestamp_10h[dpst_cut_index:]
    #     states.trsf_timestamp_10h = states.trsf_timestamp_10h[trsf_cut_index:]
    #     states.sumry_timestamp_10h = states.sumry_timestamp_10h[sumry_cut_index:]
    #     states.sumry_10h = states.sumry_10h[sumry_cut_index:]

    #     return states