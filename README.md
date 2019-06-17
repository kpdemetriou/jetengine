# JetEngine

JetEngine is a no-nonsense **Python 3.5+** asyncio [MongoDB](https://www.mongodb.com/) ODM based on [MongoEngine](http://mongoengine.org/) and [MotorEngine](https://github.com/heynemann/motorengine). This package builds upon prior work by [ilex](https://github.com/ilex) and [Björn Friedrichs](https://github.com/BFriedrichs) to introduce native Asynchronous I/O capabilities to MotorEngine.

# Installation

You can install this package using `pip` or the included `setup.py` script:

    # Using pip
    pip install jetengine
    
    # Using setup.py
    python setup.py install

# Usage

```python
import asyncio
import jetengine
from jetengine import Document, StringField


class User(Document):
    name = StringField(required=True)


async def operations():
    chris = await User(name='Chris').save()
    user = await User.objects.get(chris._id)
    assert user.name == 'Chris'

    await User.objects.create(name='Someone')
    users = await User.objects.filter(name='Someone').find_all()
    assert len(users) == 1
    assert users[0].name == 'Someone'


loop = asyncio.get_event_loop()
jetengine.connect("example", host="localhost", port=27017, io_loop=loop)
loop.run_until_complete(operations())
```

# License
```text
BSD 3-Clause License

Copyright (c) 2019, Phil Demetriou
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```