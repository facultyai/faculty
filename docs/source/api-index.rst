API
===

Clients
-------

The :func:`faculty.client` helper function is the primary interface for most
use cases. It's usually recommended to use it rather than constructing clients
directly.

.. autosummary::
   :toctree: api

   faculty.client

Modules implementing clients:

.. autosummary::
   :toctree: api

   faculty.clients.account
   faculty.clients.cluster
   faculty.clients.environment
   faculty.clients.experiment
   faculty.clients.job
   faculty.clients.log
   faculty.clients.model
   faculty.clients.object
   faculty.clients.project
   faculty.clients.report
   faculty.clients.server
   faculty.clients.user
   faculty.clients.workspace

Modules implementing common components:

.. autosummary::
   :toctree: api

   faculty.clients.auth
   faculty.clients.base

Helpers
-------

.. autosummary::
   :toctree: api

   faculty.config
   faculty.context
   faculty.session
   faculty.session.accesstoken

Datasets
--------

.. autosummary::
   :toctree: api

   faculty.datasets
   faculty.datasets.util
   faculty.datasets.transfer
