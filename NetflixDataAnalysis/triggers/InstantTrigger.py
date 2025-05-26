from pyflink.datastream import Trigger, TriggerResult, TimeWindow


class InstantTrigger(Trigger):

    def __init__(self):
        super().__init__()

    def on_element(self, element, timestamp: int, window: TimeWindow, ctx):
        if window.max_timestamp() > ctx.get_current_watermark():
            ctx.register_event_time_timer(window.max_timestamp())
        return TriggerResult.FIRE


    def on_event_time(self, time: int, window: TimeWindow, ctx):
        return TriggerResult.CONTINUE


    def on_processing_time(self, time: int, window: TimeWindow, ctx):
        return TriggerResult.CONTINUE


    def clear(self, window: TimeWindow, ctx):
        ctx.delete_event_time_timer(window.max_timestamp())

    def can_merge(self) -> bool:
        return True

    def on_merge(self, window: TimeWindow, ctx):
        if window.max_timestamp() > ctx.get_current_watermark():
            ctx.register_event_time_timer(window.max_timestamp())

    def __str__(self) -> str:
        return "InstantTrigger()"

    @staticmethod
    def create():
        return InstantTrigger()