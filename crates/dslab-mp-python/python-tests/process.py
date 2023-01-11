from dslabmp import Context, Message, Process, StateMember


class TestProcess(Process):
    def __init__(self, node_id: str):
        self.data = StateMember(["elem1", (2, 3)])
        self._id = StateMember(node_id)

        # examples of set/get state member
        self._id = 'new_node_id'
        self.secret = '''YOU SHOULDN'T SEE IT AFTER RELOAD,
                         IT IS NOT A STATE MEMBER OF NODE %s''' % self._id

    def on_local_message(self, msg: Message, ctx: Context):
        assert self._id == 'new_node_id', 'get/set attributes work bad with state members'
        tmp_value = "SHOULD BE DROPPED"
        if self.data == tmp_value:
            ctx.send_local(msg)

        # we suppose it will be discarded later
        self.data = tmp_value
        assert type(self.__dict__[
                    'data']) == StateMember, 'field "data" must be a StateMember even after assignment'

        if self.secret is None:
            pass
        else:
            ctx.send_local(msg)

        try:
            a = self.notexists
        except AttributeError:
            return
        except:
            raise 'Not a correct exception raised when addressing not-existing member'
        raise 'No exception raised when addressing not-existing member'

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from server
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass
