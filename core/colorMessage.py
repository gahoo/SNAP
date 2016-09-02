import functools


def dyeMessage(message, code):
    colors = {
        'HEADER': '\033[95m',
        'OKBLUE': '\033[94m',
        'OKGREEN': '\033[92m',
        'WARNING': '\033[93m',
        'FAIL': '\033[91m',
        'ENDC': '\033[0m',
    }
    return colors[code] + message + colors['ENDC']


dyeHEADER = functools.partial(dyeMessage, code='HEADER')
dyeOKBLUE = functools.partial(dyeMessage, code='OKBLUE')
dyeOKGREEN = functools.partial(dyeMessage, code='OKGREEN')
dyeWARNING = functools.partial(dyeMessage, code='WARNING')
dyeFAIL = functools.partial(dyeMessage, code='FAIL')
