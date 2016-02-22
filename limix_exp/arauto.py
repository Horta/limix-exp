from argparse import ArgumentParser
import config

def do_root(_):
    print(config.root_dir())

def entry_point():
    p = ArgumentParser()
    sub = p.add_subparsers()

    s = sub.add_parser('root')
    s.set_defaults(func=do_root)

    # args, rargs = p.parse_known_args()
    args = p.parse_args()
    func = args.func
    del args.func
    # func(args, rargs)
    func(args)
