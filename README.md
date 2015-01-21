# Kuyruk-Manager

See and manage Kuyruk workers.

## Install

    $ pip install kuyruk-manager

## Usage

```python
from kuyruk import Kuyruk
from kuyruk_manager import Manager

k = Kuyruk()

k.config.MANAGER_HOST = "127.0.0.1"
k.config.MANAGER_PORT = 16501
k.config.MANAGER_HTTP_PORT = 16500

m = Manager(k)


@k.task
def hello():
    print "hello"
```

Run the manager with following command:

    $ kuyruk --app ... manager
