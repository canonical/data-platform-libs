# Data Platform Libraries for Operator Framework Charms
[![Charmhub](https://charmhub.io/data-platform-libs/badge.svg)](https://charmhub.io/data-platform-libs)
[![Release](https://github.com/canonical/data-platform-libs/actions/workflows/release.yaml/badge.svg)](https://github.com/canonical/data-platform-libs/actions/workflows/release.yaml)
[![Tests](https://github.com/canonical/data-platform-libs/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/canonical/data-platform-libs/actions/workflows/ci.yaml)

## Description

The `data-platform-libs` charm provides a set of [charm libraries] which offers convenience methods
for interacting with charmed databases, but also writing you own database consuming application
charms, through [relations]

This charm is **not meant to be deployed** itself, and is used as a mechanism for hosting libraries
only.

## Usage

This charm is not intended to be deployed. It is a container for standalone charm libraries, which
can be managed using `charmcraft fetch-lib`
([ref. link](https://discourse.charmhub.io/t/how-to-find-and-use-a-charm-library/5780)), after
which they may be imported and used as normal charms. For example:

`charmcraft fetch-lib charms.data_platform_libs.v0.data_interfaces`

Following are the libraries available in this repository:

- `data_interfaces` - Library to manage the relation for the data-platform products.
- `data_models` - Library to introduce pydantic models for handling configuration, action parameters and databags.
- `database_provides` - [DEPRECATED] Library that offers custom events and methods for provider-side of the
  relation (e.g. mysql)
- `database_requires` - [DEPRECATED] Library that offers custom events and methods for requirer-side of the
  relation (e.g. wordpress)

> __Note:__ `data_interfaces` is not compatible `ops<=1.5.4`. It is compatible with only `ops>=2.0.0`. 

The charms from the `tests/integration` folder aren't meant to be used for anything beyond testing
and example code. They serve as examples of how to use the charm libraries.

## Contributing

Please see the [Juju SDK docs](https://juju.is/docs/sdk) for guidelines on enhancements to this
charm following best practice guidelines, and `CONTRIBUTING.md` for developer guidance.

<!--TODO: Add charmhub links to the libraries pages  -->

[charm libraries]: https://juju.is/docs/sdk/libraries
[relations]: https://juju.is/docs/sdk/relations
