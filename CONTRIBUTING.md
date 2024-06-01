# Contributing

## Overview

This documents explains the processes and practices recommended for contributing enhancements to
this operator.

- Generally, before developing enhancements to this charm, you should consider [opening an issue
  ](https://github.com/canonical/data-platform-libs/issues) explaining your use case.
- If you would like to chat with us about your use-cases or proposed
  implementation, you can reach us at [Canonical Mattermost public
  channel](https://chat.charmhub.io/charmhub/channels/charm-dev) or
  [Discourse](https://discourse.charmhub.io/).
- Familiarising yourself with the [Charmed Operator Framework](https://juju.is/docs/sdk) library
  will help you a lot when working on new features or bug fixes.
- All enhancements require review before being merged. Code review typically
  examines
  - code quality
  - test coverage
  - user experience for Juju administrators this charm.
- Please help us out in ensuring easy to review branches by re-basing your pull
  request branch onto the `main` branch. This also avoids merge commits and
  creates a linear Git commit history.

## Developing

You can create an environment for development with `tox`:

```shell
tox devenv -e integration
source venv/bin/activate
```

### Testing

```shell
tox run -e format        # update your code according to linting rules
tox run -e lint          # code style
tox run -e unit          # unit tests
tox run -e integration-*   # integration tests
tox                      # runs 'lint' and 'unit' environments
```

### Adding new tests

In case your tests are re-using existing test charms with no modifications, feel free to ignore this section.

For test charms that may support multiple OS versions (typically for libraries that are expected to work across
legacy versions) the following mechanism is available.

By default, with no parameters the first OS version is taken, that's specified in the helper charm's `charmcraft.yaml`.
The current default is `jammy` with `base_index: 0` (i.e. being specified as the first `build-on/run-on` environment).
Ordering goes in a way that newest version comes first, older ones decreasingly after.
(This allows for meaningful defaults for helper charms for libs that only support newer/latest OS series.)

In case any further OS versions are to be used when executing the tests, the following `pytest` parameters are to be added
at execution time
 - `dp_libs_series`: The name of the Ubuntu series to be used for build/deploy (default: `jammy`)
 - `do_libs_base_index`: The number of item in order (counting from `0`) referring to the `db_libs_series` specified in the
    helper charm's `charmcraft.yaml`

NOTE: In case using the mechanism above, make sure that the `dp_libs_ubuntu_series` fixture's value is passed for the
`series` option to your charm when deploying it in the test pipeline execution (typically: the `build_and_deploy` test function).


## Build charm

Build the charm in this git repository using:

```shell
charmcraft pack
```

### Deploy

This charm is **not meant to be deployed** itself, and is used as a mechanism
for hosting libraries only.


## Canonical Contributor Agreement


Canonical welcomes contributions to the Charmed Template Operator. Please check
out our [contributor agreement](https://ubuntu.com/legal/contributors) if you're
interested in contributing to the solution.
