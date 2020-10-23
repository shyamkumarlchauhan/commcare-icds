from functools import wraps


def return_if_no_language_code(check_on=None):
    def decorator(func):
        @wraps(func)
        def wrapped(recipient, case_schedule_instance):
            if check_on == 'recipient':
                if not recipient.get_language_code():
                    return []
            elif check_on == 'usercase':
                if not recipient.memoized_usercase or not recipient.memoized_usercase.get_language_code():
                    return []
            else:
                if not case_schedule_instance.case.get_language_code():
                    return []
            return func(recipient, case_schedule_instance)
        return wrapped
    return decorator
