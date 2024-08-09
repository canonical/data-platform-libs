# Copyright 2023 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

r"""Library to manage the relation for the data-platform products."""

import logging
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from ops.charm import (
    CharmBase,
    RelationChangedEvent,
    SecretChangedEvent,
)
from ops.model import Application, Model, ModelError, Relation, Secret, SecretNotFoundError, Unit

from .data_interfaces import (
    GROUP_SEPARATOR,
    SECRET_GROUPS,
    Data,
    IllegalOperationError,
    ProviderData,
    RequirerData,
    RequirerEventHandlers,
    Scope,
    SecretAlreadyExistsError,
    SecretGroup,
    SecretsUnavailableError,
    dynamic_secrets_only,
    juju_secrets_only,
)
from .data_interfaces import (
    CachedSecret as OrigCachedSecret,
)

# The unique Charmhub library identifier, never change it
LIBID = "blabla"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 39

PYDEPS = ["ops>=2.0.0"]

# Starting from what LIBPATCH number to apply legacy solutions
# v0.17 was the last version without secrets
LEGACY_SUPPORT_FROM = 17

logger = logging.getLogger(__name__)


# Starting from what LIBPATCH number to apply legacy solutions
# v0.17 was the last version without secrets
LEGACY_SUPPORT_FROM = 17

MODEL_ERRORS = {
    "not_leader": "this unit is not the leader",
    "no_label_and_uri": "ERROR either URI or label should be used for getting an owned secret but not both",
    "owner_no_refresh": "ERROR secret owner cannot use --refresh",
}

##############################################################################
# Global helpers / utilities
##############################################################################


def legacy_apply_from_version(version: int) -> Callable:
    """Decorator to decide whether to apply a legacy function or not.

    Based on LEGACY_SUPPORT_FROM module variable value, the importer charm may only want
    to apply legacy solutions starting from a specific LIBPATCH.

    NOTE: All 'legacy' functions have to be defined and called in a way that they return `None`.
    This results in cleaner and more secure execution flows in case the function may be disabled.
    This requirement implicitly means that legacy functions change the internal state strictly,
    don't return information.
    """

    def decorator(f: Callable[..., None]):
        """Signature is ensuring None return value."""
        f.legacy_version = version

        def wrapper(self, *args, **kwargs) -> None:
            if version >= LEGACY_SUPPORT_FROM:
                return f(self, *args, **kwargs)

        return wrapper

    return decorator


def either_static_or_dynamic_secrets(f):
    """Decorator to ensure that static and dynamic secrets won't be used in parallel."""

    def wrapper(self, *args, **kwargs):
        if self.static_secret_fields and set(self.current_secret_fields) - set(
            self.static_secret_fields
        ):
            raise IllegalOperationError(
                "Unsafe usage of statically and dynamically defined secrets, aborting."
            )
        return f(self, *args, **kwargs)

    return wrapper


################################################################################
# Peer Relation Data
################################################################################


class CachedSecret(OrigCachedSecret):
    """Locally cache a secret.

    The data structure is precisely re-using/simulating as in the actual Secret Storage
    """

    KNOWN_MODEL_ERRORS = [MODEL_ERRORS["no_label_and_uri"], MODEL_ERRORS["owner_no_refresh"]]

    def __init__(
        self,
        model: Model,
        component: Union[Application, Unit],
        label: str,
        secret_uri: Optional[str] = None,
        legacy_labels: List[str] = [],
    ):
        self._secret_meta = None
        self._secret_content = {}
        self._secret_uri = secret_uri
        self.label = label
        self._model = model
        self.component = component
        self.legacy_labels = legacy_labels
        self.current_label = None

    @property
    def meta(self) -> Optional[Secret]:
        """Getting cached secret meta-information."""
        if not self._secret_meta:
            if not (self._secret_uri or self.label):
                return

            try:
                self._secret_meta = self._model.get_secret(label=self.label)
            except SecretNotFoundError:
                # Falling back to seeking for potential legacy labels
                self._legacy_compat_find_secret_by_old_label()

            # If still not found, to be checked by URI, to be labelled with the proposed label
            if not self._secret_meta and self._secret_uri:
                self._secret_meta = self._model.get_secret(id=self._secret_uri, label=self.label)
        return self._secret_meta

    ##########################################################################
    # Backwards compatibility / Upgrades
    ##########################################################################
    # These functions are used to keep backwards compatibility on rolling upgrades
    # Policy:
    # All data is kept intact until the first write operation. (This allows a minimal
    # grace period during which rollbacks are fully safe. For more info see the spec.)
    # All data involves:
    #   - databag contents
    #   - secrets content
    #   - secret labels (!!!)
    # Legacy functions must return None, and leave an equally consistent state whether
    # they are executed or skipped (as a high enough versioned execution environment may
    # not require so)

    # Compatibility

    @legacy_apply_from_version(34)
    def _legacy_compat_find_secret_by_old_label(self) -> None:
        """Compatibility function, allowing to find a secret by a legacy label.

        This functionality is typically needed when secret labels changed over an upgrade.
        Until the first write operation, we need to maintain data as it was, including keeping
        the old secret label. In order to keep track of the old label currently used to access
        the secret, and additional 'current_label' field is being defined.
        """
        for label in self.legacy_labels:
            try:
                self._secret_meta = self._model.get_secret(label=label)
            except SecretNotFoundError:
                pass
            else:
                if label != self.label:
                    self.current_label = label
                return

    # Migrations

    @legacy_apply_from_version(34)
    def _legacy_migration_to_new_label_if_needed(self) -> None:
        """Helper function to re-create the secret with a different label.

        Juju does not provide a way to change secret labels.
        Thus whenever moving from secrets version that involves secret label changes,
        we "re-create" the existing secret, and attach the new label to the new
        secret, to be used from then on.

        Note: we replace the old secret with a new one "in place", as we can't
        easily switch the containing SecretCache structure to point to a new secret.
        Instead we are changing the 'self' (CachedSecret) object to point to the
        new instance.
        """
        if not self.current_label or not (self.meta and self._secret_meta):
            return

        # Create a new secret with the new label
        content = self._secret_meta.get_content()
        self._secret_uri = None

        # It will be nice to have the possibility to check if we are the owners of the secret...
        try:
            self._secret_meta = self.add_secret(content, label=self.label)
        except ModelError as err:
            if MODEL_ERRORS["not_leader"] not in str(err):
                raise
        self.current_label = None

    ##########################################################################
    # Public functions
    ##########################################################################

    def set_content(self, content: Dict[str, str]) -> None:
        """Setting cached secret content."""
        if not self.meta:
            return

        # DPE-4182: do not create new revision if the content stay the same
        if content == self.get_content():
            return

        if content:
            self._legacy_migration_to_new_label_if_needed()
            self.meta.set_content(content)
            self._secret_content = content
        else:
            self.meta.remove_all_revisions()


class SecretCache:
    """A data structure storing CachedSecret objects."""

    def __init__(self, model: Model, component: Union[Application, Unit]):
        self._model = model
        self.component = component
        self._secrets: Dict[str, CachedSecret] = {}

    def get(
        self, label: str, uri: Optional[str] = None, legacy_labels: List[str] = []
    ) -> Optional[CachedSecret]:
        """Getting a secret from Juju Secret store or cache."""
        if not self._secrets.get(label):
            secret = CachedSecret(
                self._model, self.component, label, uri, legacy_labels=legacy_labels
            )
            if secret.meta:
                self._secrets[label] = secret
        return self._secrets.get(label)

    def add(self, label: str, content: Dict[str, str], relation: Relation) -> CachedSecret:
        """Adding a secret to Juju Secret."""
        if self._secrets.get(label):
            raise SecretAlreadyExistsError(f"Secret {label} already exists")

        secret = CachedSecret(self._model, self.component, label)
        secret.add_secret(content, relation)
        self._secrets[label] = secret
        return self._secrets[label]

    def remove(self, label: str) -> None:
        """Remove a secret from the cache."""
        if secret := self.get(label):
            try:
                secret.remove()
                self._secrets.pop(label)
            except (SecretsUnavailableError, KeyError):
                pass
            else:
                return
        logging.debug("Non-existing Juju Secret was attempted to be removed %s", label)


class DataPeerData(RequirerData, ProviderData):
    """Represents peer relations data."""

    SECRET_FIELDS = []
    SECRET_FIELD_NAME = "internal_secret"
    SECRET_LABEL_MAP = {}

    def __init__(
        self,
        model,
        relation_name: str,
        extra_user_roles: Optional[str] = None,
        additional_secret_fields: Optional[List[str]] = [],
        additional_secret_group_mapping: Dict[str, str] = {},
        secret_field_name: Optional[str] = None,
        deleted_label: Optional[str] = None,
    ):
        RequirerData.__init__(
            self,
            model,
            relation_name,
            extra_user_roles,
            additional_secret_fields,
        )
        self.secrets = SecretCache(self._model, self.component)
        self.secret_field_name = secret_field_name if secret_field_name else self.SECRET_FIELD_NAME
        self.deleted_label = deleted_label
        self._secret_label_map = {}

        # Legacy information holders
        self._legacy_labels = []
        self._legacy_secret_uri = None

        # Secrets that are being dynamically added within the scope of this event handler run
        self._new_secrets = []
        self._additional_secret_group_mapping = additional_secret_group_mapping

        for group, fields in additional_secret_group_mapping.items():
            if group not in SECRET_GROUPS.groups():
                setattr(SECRET_GROUPS, group, group)
            for field in fields:
                secret_group = SECRET_GROUPS.get_group(group)
                internal_field = self._field_to_internal_name(field, secret_group)
                self._secret_label_map.setdefault(group, []).append(internal_field)
                self._secret_fields.append(internal_field)

    @property
    def scope(self) -> Optional[Scope]:
        """Turn component information into Scope."""
        if isinstance(self.component, Application):
            return Scope.APP
        if isinstance(self.component, Unit):
            return Scope.UNIT

    @property
    def secret_label_map(self) -> Dict[str, str]:
        """Property storing secret mappings."""
        return self._secret_label_map

    @property
    def static_secret_fields(self) -> List[str]:
        """Re-definition of the property in a way that dynamically extended list is retrieved."""
        return self._secret_fields

    @property
    def secret_fields(self) -> List[str]:
        """Re-definition of the property in a way that dynamically extended list is retrieved."""
        return (
            self.static_secret_fields if self.static_secret_fields else self.current_secret_fields
        )

    @property
    def current_secret_fields(self) -> List[str]:
        """Helper method to get all currently existing secret fields (added statically or dynamically)."""
        if not self.secrets_enabled:
            return []

        if len(self._model.relations[self.relation_name]) > 1:
            raise ValueError(f"More than one peer relation on {self.relation_name}")

        relation = self._model.relations[self.relation_name][0]
        fields = []

        ignores = [SECRET_GROUPS.get_group("user"), SECRET_GROUPS.get_group("tls")]
        for group in SECRET_GROUPS.groups():
            if group in ignores:
                continue
            if content := self._get_group_secret_contents(relation, group):
                fields += list(content.keys())
        return list(set(fields) | set(self._new_secrets))

    @dynamic_secrets_only
    def set_secret(
        self,
        relation_id: int,
        field: str,
        value: str,
        group_mapping: Optional[SecretGroup] = None,
    ) -> None:
        """Public interface method to add a Relation Data field specifically as a Juju Secret.

        Args:
            relation_id: ID of the relation
            field: The secret field that is to be added
            value: The string value of the secret
            group_mapping: The name of the "secret group", in case the field is to be added to an existing secret
        """
        self._legacy_apply_on_update([field])

        full_field = self._field_to_internal_name(field, group_mapping)
        if self.secrets_enabled and full_field not in self.current_secret_fields:
            self._new_secrets.append(full_field)
        if self.valid_field_pattern(field, full_field):
            self.update_relation_data(relation_id, {full_field: value})

    # Unlike for set_secret(), there's no harm using this operation with static secrets
    # The restricion is only added to keep the concept clear
    @dynamic_secrets_only
    def get_secret(
        self,
        relation_id: int,
        field: str,
        group_mapping: Optional[SecretGroup] = None,
    ) -> Optional[str]:
        """Public interface method to fetch secrets only."""
        self._legacy_apply_on_fetch()

        full_field = self._field_to_internal_name(field, group_mapping)
        if (
            self.secrets_enabled
            and full_field not in self.current_secret_fields
            and field not in self.current_secret_fields
        ):
            return
        if self.valid_field_pattern(field, full_field):
            return self.fetch_my_relation_field(relation_id, full_field)

    @dynamic_secrets_only
    def delete_secret(
        self,
        relation_id: int,
        field: str,
        group_mapping: Optional[SecretGroup] = None,
    ) -> Optional[str]:
        """Public interface method to delete secrets only."""
        self._legacy_apply_on_delete([field])

        full_field = self._field_to_internal_name(field, group_mapping)
        if self.secrets_enabled and full_field not in self.current_secret_fields:
            logger.warning(f"Secret {field} from group {group_mapping} was not found")
            return

        if self.valid_field_pattern(field, full_field):
            self.delete_relation_data(relation_id, [full_field])

    ##########################################################################
    # Helpers
    ##########################################################################

    @staticmethod
    def _field_to_internal_name(field: str, group: Optional[SecretGroup]) -> str:
        if not group or group == SECRET_GROUPS.EXTRA:
            return field
        return f"{field}{GROUP_SEPARATOR}{group}"

    @staticmethod
    def _internal_name_to_field(name: str) -> Tuple[str, SecretGroup]:
        parts = name.split(GROUP_SEPARATOR)
        if not len(parts) > 1:
            return (parts[0], SECRET_GROUPS.EXTRA)
        secret_group = SECRET_GROUPS.get_group(parts[1])
        if not secret_group:
            raise ValueError(f"Invalid secret field {name}")
        return (parts[0], secret_group)

    def _group_secret_fields(self, secret_fields: List[str]) -> Dict[SecretGroup, List[str]]:
        """Helper function to arrange secret mappings under their group.

        NOTE: All unrecognized items end up in the 'extra' secret bucket.
        Make sure only secret fields are passed!
        """
        secret_fieldnames_grouped = {}
        for key in secret_fields:
            field, group = self._internal_name_to_field(key)
            secret_fieldnames_grouped.setdefault(group, []).append(field)
        return secret_fieldnames_grouped

    def _content_for_secret_group(
        self, content: Dict[str, str], secret_fields: Set[str], group_mapping: SecretGroup
    ) -> Dict[str, str]:
        """Select <field>: <value> pairs from input, that belong to this particular Secret group."""
        if group_mapping == SECRET_GROUPS.EXTRA:
            return {k: v for k, v in content.items() if k in self.secret_fields}
        return {
            self._internal_name_to_field(k)[0]: v
            for k, v in content.items()
            if k in self.secret_fields
        }

    def valid_field_pattern(self, field: str, full_field: str) -> bool:
        """Check that no secret group is attempted to be used together without secrets being enabled.

        Secrets groups are impossible to use with versions that are not yet supporting secrets.
        """
        if not self.secrets_enabled and full_field != field:
            logger.error(
                f"Can't access {full_field}: no secrets available (i.e. no secret groups either)."
            )
            return False
        return True

    ##########################################################################
    # Backwards compatibility / Upgrades
    ##########################################################################
    # These functions are used to keep backwards compatibility on upgrades
    # Policy:
    # All data is kept intact until the first write operation. (This allows a minimal
    # grace period during which rollbacks are fully safe. For more info see spec.)
    # All data involves:
    #   - databag
    #   - secrets content
    #   - secret labels (!!!)
    # Legacy functions must return None, and leave an equally consistent state whether
    # they are executed or skipped (as a high enough versioned execution environment may
    # not require so)

    # Full legacy stack for each operation

    def _legacy_apply_on_fetch(self) -> None:
        """All legacy functions to be applied on fetch."""
        relation = self._model.relations[self.relation_name][0]
        self._legacy_compat_generate_prev_labels()
        self._legacy_compat_secret_uri_from_databag(relation)

    def _legacy_apply_on_update(self, fields) -> None:
        """All legacy functions to be applied on update."""
        relation = self._model.relations[self.relation_name][0]
        self._legacy_compat_generate_prev_labels()
        self._legacy_compat_secret_uri_from_databag(relation)
        self._legacy_migration_remove_secret_from_databag(relation, fields)
        self._legacy_migration_remove_secret_field_name_from_databag(relation)

    def _legacy_apply_on_delete(self, fields) -> None:
        """All legacy functions to be applied on delete."""
        relation = self._model.relations[self.relation_name][0]
        self._legacy_compat_generate_prev_labels()
        self._legacy_compat_secret_uri_from_databag(relation)
        self._legacy_compat_check_deleted_label(relation, fields)

    # Compatibility

    @legacy_apply_from_version(18)
    def _legacy_compat_check_deleted_label(self, relation, fields) -> None:
        """Helper function for legacy behavior.

        As long as https://bugs.launchpad.net/juju/+bug/2028094 wasn't fixed,
        we did not delete fields but rather kept them in the secret with a string value
        expressing invalidity. This function is maintainnig that behavior when needed.
        """
        if not self.deleted_label:
            return

        current_data = self.fetch_my_relation_data([relation.id], fields)
        if current_data is not None:
            # Check if the secret we wanna delete actually exists
            # Given the "deleted label", here we can't rely on the default mechanism (i.e. 'key not found')
            if non_existent := (set(fields) & set(self.secret_fields)) - set(
                current_data.get(relation.id, [])
            ):
                logger.debug(
                    "Non-existing secret %s was attempted to be removed.",
                    ", ".join(non_existent),
                )

    @legacy_apply_from_version(18)
    def _legacy_compat_secret_uri_from_databag(self, relation) -> None:
        """Fetching the secret URI from the databag, in case stored there."""
        self._legacy_secret_uri = relation.data[self.component].get(
            self._generate_secret_field_name(), None
        )

    @legacy_apply_from_version(34)
    def _legacy_compat_generate_prev_labels(self) -> None:
        """Generator for legacy secret label names, for backwards compatibility.

        Secret label is part of the data that MUST be maintained across rolling upgrades.
        In case there may be a change on a secret label, the old label must be recognized
        after upgrades, and left intact until the first write operation -- when we roll over
        to the new label.

        This function keeps "memory" of previously used secret labels.
        NOTE: Return value takes decorator into account -- all 'legacy' functions may return `None`

        v0.34 (rev69): Fixing issue https://github.com/canonical/data-platform-libs/issues/155
                       meant moving from '<app_name>.<scope>' (i.e. 'mysql.app', 'mysql.unit')
                       to labels '<relation_name>.<app_name>.<scope>' (like 'peer.mysql.app')
        """
        if self._legacy_labels:
            return

        result = []
        members = [self._model.app.name]
        if self.scope:
            members.append(self.scope.value)
        result.append(f"{'.'.join(members)}")
        self._legacy_labels = result

    # Migration

    @legacy_apply_from_version(18)
    def _legacy_migration_remove_secret_from_databag(self, relation, fields: List[str]) -> None:
        """For Rolling Upgrades -- when moving from databag to secrets usage.

        Practically what happens here is to remove stuff from the databag that is
        to be stored in secrets.
        """
        if not self.secret_fields:
            return

        secret_fields_passed = set(self.secret_fields) & set(fields)
        for field in secret_fields_passed:
            if self._fetch_relation_data_without_secrets(self.component, relation, [field]):
                self._delete_relation_data_without_secrets(self.component, relation, [field])

    @legacy_apply_from_version(18)
    def _legacy_migration_remove_secret_field_name_from_databag(self, relation) -> None:
        """Making sure that the old databag URI is gone.

        This action should not be executed more than once.

        There was a phase (before moving secrets usage to libs) when charms saved the peer
        secret URI to the databag, and used this URI from then on to retrieve their secret.
        When upgrading to charm versions using this library, we need to add a label to the
        secret and access it via label from than on, and remove the old traces from the databag.
        """
        # Nothing to do if 'internal-secret' is not in the databag
        if not (relation.data[self.component].get(self._generate_secret_field_name())):
            return

        # Making sure that the secret receives its label
        # (This should have happened by the time we get here, rather an extra security measure.)
        secret = self._get_relation_secret(relation.id)

        # Either app scope secret with leader executing, or unit scope secret
        leader_or_unit_scope = self.component != self.local_app or self.local_unit.is_leader()
        if secret and leader_or_unit_scope:
            # Databag reference to the secret URI can be removed, now that it's labelled
            relation.data[self.component].pop(self._generate_secret_field_name(), None)

    ##########################################################################
    # Event handlers
    ##########################################################################

    def _on_relation_changed_event(self, event: RelationChangedEvent) -> None:
        """Event emitted when the relation has changed."""
        pass

    def _on_secret_changed_event(self, event: SecretChangedEvent) -> None:
        """Event emitted when the secret has changed."""
        pass

    ##########################################################################
    # Overrides of Relation Data handling functions
    ##########################################################################

    def _generate_secret_label(
        self, relation_name: str, relation_id: int, group_mapping: SecretGroup
    ) -> str:
        members = [relation_name, self._model.app.name]
        if self.scope:
            members.append(self.scope.value)
        if group_mapping != SECRET_GROUPS.EXTRA:
            members.append(group_mapping)
        return f"{'.'.join(members)}"

    def _generate_secret_field_name(self, group_mapping: SecretGroup = SECRET_GROUPS.EXTRA) -> str:
        """Generate unique group_mappings for secrets within a relation context."""
        return f"{self.secret_field_name}"

    @juju_secrets_only
    def _get_relation_secret(
        self,
        relation_id: int,
        group_mapping: SecretGroup = SECRET_GROUPS.EXTRA,
        relation_name: Optional[str] = None,
    ) -> Optional[CachedSecret]:
        """Retrieve a Juju Secret specifically for peer relations.

        In case this code may be executed within a rolling upgrade, and we may need to
        migrate secrets from the databag to labels, we make sure to stick the correct
        label on the secret, and clean up the local databag.
        """
        if not relation_name:
            relation_name = self.relation_name

        relation = self._model.get_relation(relation_name, relation_id)
        if not relation:
            return

        label = self._generate_secret_label(relation_name, relation_id, group_mapping)

        # URI or legacy label is only to applied when moving single legacy secret to a (new) label
        if group_mapping == SECRET_GROUPS.EXTRA:
            # Fetching the secret with fallback to URI (in case label is not yet known)
            # Label would we "stuck" on the secret in case it is found
            return self.secrets.get(
                label, self._legacy_secret_uri, legacy_labels=self._legacy_labels
            )
        return self.secrets.get(label)

    def _get_group_secret_contents(
        self,
        relation: Relation,
        group: SecretGroup,
        secret_fields: Union[Set[str], List[str]] = [],
    ) -> Dict[str, str]:
        """Helper function to retrieve collective, requested contents of a secret."""
        secret_fields = [self._internal_name_to_field(k)[0] for k in secret_fields]
        result = super()._get_group_secret_contents(relation, group, secret_fields)
        if self.deleted_label:
            result = {key: result[key] for key in result if result[key] != self.deleted_label}
        if self._additional_secret_group_mapping:
            return {self._field_to_internal_name(key, group): result[key] for key in result}
        return result

    @either_static_or_dynamic_secrets
    def _fetch_my_specific_relation_data(
        self, relation: Relation, fields: Optional[List[str]]
    ) -> Dict[str, str]:
        """Fetch data available (directily or indirectly -- i.e. secrets) from the relation for owner/this_app."""
        return self._fetch_relation_data_with_secrets(
            self.component, self.secret_fields, relation, fields
        )

    @either_static_or_dynamic_secrets
    def _update_relation_data(self, relation: Relation, data: Dict[str, str]) -> None:
        """Update data available (directily or indirectly -- i.e. secrets) from the relation for owner/this_app."""
        _, normal_fields = self._process_secret_fields(
            relation,
            self.secret_fields,
            list(data),
            self._add_or_update_relation_secrets,
            data=data,
            uri_to_databag=False,
        )

        normal_content = {k: v for k, v in data.items() if k in normal_fields}
        self._update_relation_data_without_secrets(self.component, relation, normal_content)

    @either_static_or_dynamic_secrets
    def _delete_relation_data(self, relation: Relation, fields: List[str]) -> None:
        """Delete data available (directily or indirectly -- i.e. secrets) from the relation for owner/this_app."""
        if self.secret_fields and self.deleted_label:

            _, normal_fields = self._process_secret_fields(
                relation,
                self.secret_fields,
                fields,
                self._update_relation_secret,
                data={field: self.deleted_label for field in fields},
            )
        else:
            _, normal_fields = self._process_secret_fields(
                relation, self.secret_fields, fields, self._delete_relation_secret, fields=fields
            )
        self._delete_relation_data_without_secrets(self.component, relation, list(normal_fields))

    def fetch_relation_data(
        self,
        relation_ids: Optional[List[int]] = None,
        fields: Optional[List[str]] = None,
        relation_name: Optional[str] = None,
    ) -> Dict[int, Dict[str, str]]:
        """This method makes no sense for a Peer Relation."""
        raise NotImplementedError(
            "Peer Relation only supports 'self-side' fetch methods: "
            "fetch_my_relation_data() and fetch_my_relation_field()"
        )

    def fetch_relation_field(
        self, relation_id: int, field: str, relation_name: Optional[str] = None
    ) -> Optional[str]:
        """This method makes no sense for a Peer Relation."""
        raise NotImplementedError(
            "Peer Relation only supports 'self-side' fetch methods: "
            "fetch_my_relation_data() and fetch_my_relation_field()"
        )

    ##########################################################################
    # Public functions -- inherited
    ##########################################################################

    fetch_my_relation_data = Data.fetch_my_relation_data
    fetch_my_relation_field = Data.fetch_my_relation_field


class DataPeerEventHandlers(RequirerEventHandlers):
    """Requires-side of the relation."""

    def __init__(self, charm: CharmBase, relation_data: RequirerData, unique_key: str = ""):
        """Manager of base client relations."""
        super().__init__(charm, relation_data, unique_key)

    def _on_relation_changed_event(self, event: RelationChangedEvent) -> None:
        """Event emitted when the relation has changed."""
        pass

    def _on_secret_changed_event(self, event: SecretChangedEvent) -> None:
        """Event emitted when the secret has changed."""
        pass


class DataPeer(DataPeerData, DataPeerEventHandlers):
    """Represents peer relations."""

    def __init__(
        self,
        charm,
        relation_name: str,
        extra_user_roles: Optional[str] = None,
        additional_secret_fields: Optional[List[str]] = [],
        additional_secret_group_mapping: Dict[str, str] = {},
        secret_field_name: Optional[str] = None,
        deleted_label: Optional[str] = None,
        unique_key: str = "",
    ):
        DataPeerData.__init__(
            self,
            charm.model,
            relation_name,
            extra_user_roles,
            additional_secret_fields,
            additional_secret_group_mapping,
            secret_field_name,
            deleted_label,
        )
        DataPeerEventHandlers.__init__(self, charm, self, unique_key)


class DataPeerUnitData(DataPeerData):
    """Unit data abstraction representation."""

    SCOPE = Scope.UNIT

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DataPeerUnit(DataPeerUnitData, DataPeerEventHandlers):
    """Unit databag representation."""

    def __init__(
        self,
        charm,
        relation_name: str,
        extra_user_roles: Optional[str] = None,
        additional_secret_fields: Optional[List[str]] = [],
        additional_secret_group_mapping: Dict[str, str] = {},
        secret_field_name: Optional[str] = None,
        deleted_label: Optional[str] = None,
        unique_key: str = "",
    ):
        DataPeerData.__init__(
            self,
            charm.model,
            relation_name,
            extra_user_roles,
            additional_secret_fields,
            additional_secret_group_mapping,
            secret_field_name,
            deleted_label,
        )
        DataPeerEventHandlers.__init__(self, charm, self, unique_key)


class DataPeerOtherUnitData(DataPeerUnitData):
    """Unit data abstraction representation."""

    def __init__(self, unit: Unit, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_unit = unit
        self.component = unit

    def update_relation_data(self, relation_id: int, data: dict) -> None:
        """This method makes no sense for a Other Peer Relation."""
        raise NotImplementedError("It's not possible to update data of another unit.")

    def delete_relation_data(self, relation_id: int, fields: List[str]) -> None:
        """This method makes no sense for a Other Peer Relation."""
        raise NotImplementedError("It's not possible to delete data of another unit.")


class DataPeerOtherUnitEventHandlers(DataPeerEventHandlers):
    """Requires-side of the relation."""

    def __init__(self, charm: CharmBase, relation_data: DataPeerUnitData):
        """Manager of base client relations."""
        unique_key = f"{relation_data.relation_name}-{relation_data.local_unit.name}"
        super().__init__(charm, relation_data, unique_key=unique_key)


class DataPeerOtherUnit(DataPeerOtherUnitData, DataPeerOtherUnitEventHandlers):
    """Unit databag representation for another unit than the executor."""

    def __init__(
        self,
        unit: Unit,
        charm: CharmBase,
        relation_name: str,
        extra_user_roles: Optional[str] = None,
        additional_secret_fields: Optional[List[str]] = [],
        additional_secret_group_mapping: Dict[str, str] = {},
        secret_field_name: Optional[str] = None,
        deleted_label: Optional[str] = None,
    ):
        DataPeerOtherUnitData.__init__(
            self,
            unit,
            charm.model,
            relation_name,
            extra_user_roles,
            additional_secret_fields,
            additional_secret_group_mapping,
            secret_field_name,
            deleted_label,
        )
        DataPeerOtherUnitEventHandlers.__init__(self, charm, self)
