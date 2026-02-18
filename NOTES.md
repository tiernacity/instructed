No actual server process in the todo app!

Check how is optimistic locking enforced

Check command vs projection handling is separate, assuming no strong consistency request. projections are eventually consistent by default

Compare feature set w/ commanded, and check test coverage of features. Including:

- optimistic locking
- control over strong vs eventual consistency
- routing commands to aggregates by id attribute
- everything else
