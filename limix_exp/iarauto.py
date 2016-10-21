
import cmd
from . import arauto
import shlex

class IArauto(cmd.Cmd):
    def do_einfo(self, cmdline):
        arauto.parse_einfo(shlex.split(cmdline))

    def do_root(self, _):
        arauto.do_root()

    def do_see(self, cmdline):
        arauto.parse_see(shlex.split(cmdline))

    def do_jinfo(self, cmdline):
        arauto.parse_jinfo(shlex.split(cmdline))

    def do_sjobs(self, cmdline):
        arauto.parse_sjobs(shlex.split(cmdline))

    def do_winfo(self, cmdline):
        arauto.parse_winfo(shlex.split(cmdline))

    def do_rjob(self, cmdline):
        arauto.parse_rjob(shlex.split(cmdline))

    def do_rm_exp(self, cmdline):
        arauto.parse_rm_exp(shlex.split(cmdline))

    def do_err(self, cmdline):
        arauto.parse_err(shlex.split(cmdline))

    def do_EOF(self, _):
        return True

    def do_exit(self, *_):
        return True

def entry_point():
    IArauto().cmdloop()

if __name__ == '__main__':
    entry_point()
