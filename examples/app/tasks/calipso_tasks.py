from time import sleep

from ..app import app


@app.task(name='calipso.function')
def function(noOfDays=None):
    print(f'Task called with noOfDays-{noOfDays}')
    sleep(30)
    if noOfDays == 0:
        raise Exception('noOfDays is 0')
