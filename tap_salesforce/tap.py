"""Salesforce tap class."""
from __future__ import annotations

import datetime
from functools import lru_cache
from typing import List, Optional

from simple_salesforce import Salesforce
from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk._singerlib.catalog import Catalog, CatalogEntry, AnyMetadata


BULK_API_TYPE = "BULK"
REST_API_TYPE = "REST"

STRING_TYPES = set(
    [
        "id",
        "string",
        "picklist",
        "textarea",
        "phone",
        "url",
        "reference",
        "multipicklist",
        "combobox",
        "encryptedstring",
        "email",
        "complexvalue",  # TODO: Unverified
        "masterrecord",
        "datacategorygroupreference",
    ]
)

NUMBER_TYPES = set(["double", "currency", "percent"])

DATE_TYPES = set(["datetime", "date"])

BINARY_TYPES = set(["base64", "byte"])

LOOSE_TYPES = set(
    [
        "anyType",
        # A calculated field's type can be any of the supported
        # formula data types (see https://developer.salesforce.com/docs/#i1435527)
        "calculated",
    ]
)

# The following objects are not supported by the bulk API.
UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS = set(
    [
        "AssetTokenEvent",
        "AttachedContentNote",
        "EventWhoRelation",
        "QuoteTemplateRichTextData",
        "TaskWhoRelation",
        "SolutionStatus",
        "ContractStatus",
        "RecentlyViewed",
        "DeclinedEventRelation",
        "AcceptedEventRelation",
        "TaskStatus",
        "PartnerRole",
        "TaskPriority",
        "CaseStatus",
        "UndecidedEventRelation",
        "OrderStatus",
    ]
)

# The following objects have certain WHERE clause restrictions so we exclude them.
QUERY_RESTRICTED_SALESFORCE_OBJECTS = set(
    [
        "Announcement",
        "ContentDocumentLink",
        "CollaborationGroupRecord",
        "Vote",
        "IdeaComment",
        "FieldDefinition",
        "PlatformAction",
        "UserEntityAccess",
        "RelationshipInfo",
        "ContentFolderMember",
        "ContentFolderItem",
        "SearchLayout",
        "SiteDetail",
        "EntityParticle",
        "OwnerChangeOptionInfo",
        "DataStatistics",
        "UserFieldAccess",
        "PicklistValueInfo",
        "RelationshipDomain",
        "FlexQueueItem",
        "NetworkUserHistoryRecent",
        "FieldHistoryArchive",
        "RecordActionHistory",
        "FlowVersionView",
        "FlowVariableView",
        "AppTabMember",
        "ColorDefinition",
        "IconDefinition",
    ]
)

# The following objects are not supported by the query method being used.
QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS = set(
    [
        "DataType",
        "ListViewChartInstance",
        "FeedLike",
        "OutgoingEmail",
        "OutgoingEmailRelation",
        "FeedSignal",
        "ActivityHistory",
        "EmailStatus",
        "UserRecordAccess",
        "Name",
        "AggregateResult",
        "OpenActivity",
        "ProcessInstanceHistory",
        "OwnedContentDocument",
        "FolderedContentDocument",
        "FeedTrackedChange",
        "CombinedAttachment",
        "AttachedContentDocument",
        "ContentBody",
        "NoteAndAttachment",
        "LookedUpFromActivity",
        "AttachedContentNote",
        "QuoteTemplateRichTextData",
    ]
)

# Does not support ordering by CreatedDate
FORCED_FULL_TABLE = {"BackgroundOperationResult"}


class SalesforceStream(Stream):
    """Salesforce stream class."""

    is_sorted = True
    STATE_MSG_FREQUENCY = 1

    def __init__(self, client: Salesforce, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client

    def get_records(self, context: dict) -> list[dict]:
        """Return a list of records."""
        # get bookmark
        bookmark = self.get_starting_timestamp(context)
        # projection
        projection = [
            prop
            for prop in self.schema["properties"]
            if prop != "attributes"
            and self.metadata[("properties", prop)].selected
            and not self.metadata[("properties", prop)].inclusion == "unsupported"
        ]
        # date-time fields need to be converted to ISO format
        hooks = {
            prop: lambda v: datetime.datetime.isoformat(
                datetime.datetime.fromtimestamp(v / 1000)
            )
            for prop in self.schema["properties"]
            if self.schema["properties"][prop].get("format") == "date-time"
        }
        # make query
        query = f"SELECT {','.join(projection)} FROM {self.name} "
        if bookmark:
            query += f"WHERE {self.replication_key} >= {bookmark.isoformat(timespec='seconds')}Z "
        if self.replication_key:
            query += f"ORDER BY {self.replication_key} ASC "
        if self._MAX_RECORDS_LIMIT:
            query += f"LIMIT {self._MAX_RECORDS_LIMIT} "
        # execute query
        self.logger.info(f"Query: {query}")
        for page in getattr(self.client.bulk, self.name).query_all(
            query, lazy_operation=True
        ):
            for record in page:
                record.pop("attributes", None)
                for hook_prop in hooks:
                    if hook_prop in record:
                        record[hook_prop] = hooks[hook_prop](record[hook_prop])
                yield record


class TapSalesforce(Tap):
    """Salesforce tap class."""

    name = "tap-salesforce"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_type",
            th.CustomType({"type": "string", "enum": [BULK_API_TYPE, REST_API_TYPE]}),
            required=True,
            description="The API type to use for this tap. BULK is recommended for large datasets.",
        ),
        th.Property(
            "select_fields_by_default",
            th.BooleanType,
            default=True,
            description=(
                "Whether to select all fields by default. If false, no fields will be selected "
                "by default and only the fields selected in the catalog will be replicated."
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The start date for the tap to replicate data from.",
        ),
        th.Property(
            "client_id",
            th.StringType,
            description="The client ID for the OAuth2 authentication flow.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            description="The client secret for the OAuth2 authentication flow.",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            description="The refresh token for the OAuth2 authentication flow.",
        ),
        th.Property(
            "username",
            th.StringType,
            description="The username for the username/password authentication flow.",
        ),
        th.Property(
            "password",
            th.StringType,
            description="The password for the username/password authentication flow.",
        ),
        th.Property(
            "security_token",
            th.StringType,
            description="The security token for the username/password authentication flow.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        client = self.get_client()
        for entry in self.catalog_dict["streams"]:
            stream = SalesforceStream(
                client=client,
                tap=self,
                schema=entry["schema"],
                name=entry["stream"],
            )
            stream.apply_catalog(Catalog.from_dict(entry))
            yield stream

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.
        Returns:
            The tap's catalog as a dict
        """
        # Use cached catalog if available
        if hasattr(self, "_catalog_dict") and self._catalog_dict:
            return self._catalog_dict
        # Defer to passed in catalog if available
        if self.input_catalog:
            return self.input_catalog.to_dict()
        # If no catalog is provided, discover streams
        client = self.get_client()
        catalog = Catalog()
        key_properties = ["Id"]
        objects_to_discover = [
            o["name"]
            for o in client.describe()["sobjects"]
            if o["queryable"]
            and o["name"] not in self.get_blacklisted_objects()
            and not o["name"].endswith("ChangeEvent")
        ]
        for sobject_name in objects_to_discover:
            entry = CatalogEntry.from_dict({"tap_stream_id": sobject_name})
            sobject = getattr(client, sobject_name).describe()
            # Get fields
            fields = sobject["fields"]
            # Salesforce objects have a field called "Id" that is the primary key
            if not any(f["name"] == "Id" for f in fields):
                self.logger.info(
                    "Skipping Salesforce Object %s, as it has no Id field", sobject_name
                )
                continue
            # Get replication key
            entry.replication_key = self.get_replication_key(sobject_name, fields)
            entry.key_properties = key_properties
            entry.metadata.root.table_key_properties = key_properties
            if entry.replication_key:
                entry.metadata.root.valid_replication_keys = [entry.replication_key]
            else:
                entry.metadata.root.valid_replication_keys = []
                entry.metadata.root.forced_replication_method = "FULL_TABLE"
            # Loop over the object's fields
            for f in fields:
                name, typ = f["name"], f["type"]
                prop_schema = self.create_property_schema(
                    name,
                    typ,
                    metadata_entry=entry.metadata[name],
                )
                if typ == "address" and self.config["api_type"] == BULK_API_TYPE:
                    entry.metadata[name].inclusion = "unsupported"
                    entry.metadata[name][
                        "unsupported-description"
                    ] = "Cannot query compound address fields with bulk API"
                if typ == "json":
                    entry.metadata[name].inclusion = "unsupported"
                    entry.metadata[name][
                        "unsupported-description"
                    ] = "JSON fields are not supported"
                pair = (sobject_name, name)
                if pair in self.get_blacklisted_fields():
                    entry.metadata[name].inclusion = "unsupported"
                    entry.metadata[name][
                        "unsupported-description"
                    ] = "Cannot query blacklisted field"
                if (
                    self.config["select_fields_by_default"]
                    and entry.metadata[name].inclusion != "unsupported"
                ):
                    entry.metadata[name].selected_by_default = True
                if entry.replication_key == name:
                    entry.metadata[name].inclusion = "automatic"
                entry.schema.properties[name] = prop_schema
                catalog.add_stream(entry)
        self._catalog_dict = catalog.to_dict()  # cache catalog
        return self._catalog_dict

    @lru_cache(maxsize=None)
    def get_client(self) -> Salesforce:
        """Return an instance of the API client."""
        # OAuth2
        if all(
            key in self.config
            for key in ("client_id", "client_secret", "refresh_token")
        ):
            import requests

            instance = self.config.get("instance", "login")
            client_id, client_secret, refresh_token = (
                self.config["client_id"],
                self.config["client_secret"],
                self.config["refresh_token"],
            )
            payload = "&".join(
                [
                    "client_id={}".format(client_id),
                    "client_secret={}".format(client_secret),
                    "grant_type=refresh_token",
                    "refresh_token={}".format(refresh_token),
                ]
            )
            headers = {"content-type": "application/x-www-form-urlencoded"}
            response = requests.request(
                "POST",
                f"https://{instance}.salesforce.com/services/oauth2/token",
                data=payload,
                headers=headers,
            )
            credentials = response.json()
            return Salesforce(
                instance_url=credentials["instance_url"],
                session_id=credentials["access_token"],
            )
        # Username/Password
        elif all(
            key in self.config for key in ("username", "password", "security_token")
        ):
            return Salesforce(
                username=self.config["username"],
                password=self.config["password"],
                security_token=self.config["security_token"],
            )
        # Invalid
        else:
            raise Exception(
                "Invalid authentication method. "
                "Please provide either a refresh token or username/password/security token."
            )

    def get_blacklisted_objects(self) -> List[str]:
        """Return a list of objects that should not be replicated."""
        if self.config["api_type"] == BULK_API_TYPE:
            return list(
                UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS
                | QUERY_RESTRICTED_SALESFORCE_OBJECTS
                | QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS
            )
        elif self.config["api_type"] == REST_API_TYPE:
            return list(
                QUERY_RESTRICTED_SALESFORCE_OBJECTS
                | QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS
            )
        else:
            raise ValueError(
                "api_type should be REST or BULK was: {}".format(self.api_type)
            )

    def get_blacklisted_fields(self):
        """Return a list of fields that should not be replicated."""
        if self.config["api_type"] == BULK_API_TYPE:
            return {
                (
                    "EntityDefinition",
                    "RecordTypesSupported",
                ): "this field is unsupported by the Bulk API."
            }
        elif self.config["api_type"] == REST_API_TYPE:
            return {}
        else:
            raise ValueError(
                "api_type should be REST or BULK was: {}".format(self.api_type)
            )

    def get_replication_key(self, sobject_name, fields) -> Optional[str]:
        """Return the replication key for the given object."""
        if sobject_name in FORCED_FULL_TABLE:
            return None
        names = [f["name"] for f in fields]
        if "SystemModstamp" in names:
            return "SystemModstamp"
        elif "LastModifiedDate" in names:
            return "LastModifiedDate"
        elif "CreatedDate" in names:
            return "CreatedDate"
        elif "LoginTime" in names and sobject_name == "LoginHistory":
            return "LoginTime"
        return None

    def create_property_schema(
        self, name: str, typ: str, metadata_entry: AnyMetadata
    ) -> dict:
        """Create a schema for a given property."""
        prop_schema = {}
        # set inclusion
        metadata_entry.inclusion = "automatic" if name == "Id" else "available"
        # resolve type
        if typ in STRING_TYPES:
            prop_schema["type"] = "string"
        elif typ in DATE_TYPES:
            date_type = {"type": "string", "format": "date-time"}
            string_type = {"type": ["string", "null"]}
            prop_schema["anyOf"] = [date_type, string_type]
        elif typ == "boolean":
            prop_schema["type"] = "boolean"
        elif typ in NUMBER_TYPES:
            prop_schema["type"] = "number"
        elif typ == "address":
            prop_schema["type"] = "object"
            prop_schema["properties"] = {
                "street": {"type": ["null", "string"]},
                "state": {"type": ["null", "string"]},
                "postalCode": {"type": ["null", "string"]},
                "city": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "longitude": {"type": ["null", "number"]},
                "latitude": {"type": ["null", "number"]},
                "geocodeAccuracy": {"type": ["null", "string"]},
            }
        elif typ in ("int", "long"):
            prop_schema["type"] = "integer"
        elif typ == "time":
            prop_schema["type"] = "string"
        elif typ in LOOSE_TYPES:
            # TODO: these are any types
            return prop_schema, metadata_entry
        elif typ in BINARY_TYPES:
            metadata_entry.inclusion = "unsupported"
            metadata_entry["unsupported-description"] = "binary data"
            return prop_schema, metadata_entry
        elif typ == "location":
            # geo coordinates are numbers or objects divided into two fields for lat/lon
            # TODO: this is too loose
            prop_schema["type"] = ["number", "object", "null"]
            prop_schema["properties"] = {
                "longitude": {"type": ["null", "number"]},
                "latitude": {"type": ["null", "number"]},
            }
        elif typ == "json":
            prop_schema["type"] = "string"
        else:
            raise ValueError("Found unsupported type: {}".format(typ))
        # ensure that most fields are nullable
        if name != "Id" and typ != "location" and typ not in DATE_TYPES:
            prop_schema["type"] = ["null", prop_schema["type"]]
        return prop_schema


if __name__ == "__main__":
    TapSalesforce.cli()
