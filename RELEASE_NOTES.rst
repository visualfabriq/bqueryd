Release 0.3.10
=======================
- Fixing unzipping error in downloader node

Release 0.3.9
=======================
- Setting up CircleCI workflows

Release 0.3.8
=======================
- Fix download error with unzipping ubication.

Release 0.3.7
=======================
- Update minimum requirements.

Release 0.3.6
=======================
- Ensure uncompressing temporary files are deleted if there is an error uncompressing.

Release 0.3.5
=======================
- Added functionality for downloading files from Azure Blob Storage.

Release 0.3.4
=======================
- Fix module import error.

Release 0.3.3
=======================
- Fix hardcoded version string in ``bqueryd.__init__``.
- Round robin over all available workers (both local and remote) in the controller unless "needs_local" is specified.

Release 0.3.2
=======================
- Fix the issue with docker container exiting when run through docker-compose.

Release 0.3.1
=======================
- Add docker-compose to containerize the project.
- Add circleci config.
- Add tests for downloader, rpc methods and movebcolz.
- Simplify node restart check.

Release  0.3.0
=======================
- Move to boto3

Release  0.2.1
=======================
- Manifest fix

Release  0.2.0
=======================
- Initial release

.. Local Variables:
.. mode: rst
.. coding: utf-8
.. fill-column: 72
.. End:
