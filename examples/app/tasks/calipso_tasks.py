from ..app import app

@app.task(name='calipso.function')
def function(noOfDays=None):
    print(f'Task called with noOfDays-{noOfDays}')
