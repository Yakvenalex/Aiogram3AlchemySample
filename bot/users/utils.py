def get_refer_id_or_none(command_args, user_id):
    return int(command_args) if command_args and command_args.isdigit() and int(command_args) > 0 and int(
        command_args) != user_id else None
