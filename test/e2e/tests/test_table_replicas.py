# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
# 	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the DynamoDB Table Replicas.
"""

import logging
import time
from typing import Dict, Tuple

import boto3
import pytest
from acktest import tags
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import (CRD_GROUP, CRD_VERSION, condition, get_resource_tags,
                 load_dynamodb_resource, service_marker, table,
                 wait_for_cr_status)
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "tables"

DELETE_WAIT_AFTER_SECONDS = 15
MODIFY_WAIT_AFTER_SECONDS = 90
REPLICA_WAIT_AFTER_SECONDS = 300  # Replicas can take longer to create/update

REPLICA_REGION_1 = "us-east-1"
REPLICA_REGION_2 = "eu-west-1"


def create_table_with_replicas(name: str, resource_template, regions=None):
    if regions is None:
        regions = [REPLICA_REGION_1]

    replacements = REPLACEMENT_VALUES.copy()
    replacements["TABLE_NAME"] = name
    replacements["REPLICA_REGION_1"] = regions[0]

    resource_data = load_dynamodb_resource(
        resource_template,
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    # Create the k8s resource
    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    return (ref, cr)


@pytest.fixture(scope="module")
def table_with_replicas():
    table_name = random_suffix_name("table-replicas", 32)

    (ref, res) = create_table_with_replicas(
        table_name,
        "table_with_replicas",
        [REPLICA_REGION_1]
    )

    yield (ref, res)

    # Delete the k8s resource if it still exists
    if k8s.get_resource_exists(ref):
        k8s.delete_custom_resource(ref)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)


def create_table_with_invalid_replicas(name: str):
    replacements = REPLACEMENT_VALUES.copy()
    replacements["TABLE_NAME"] = name
    replacements["REPLICA_REGION_1"] = REPLICA_REGION_1

    resource_data = load_dynamodb_resource(
        "table_with_replicas_invalid",
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    # Create the k8s resource
    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    return (ref, cr)


@pytest.fixture(scope="function")
def table_with_invalid_replicas():
    table_name = random_suffix_name("table-invalid-replicas", 32)

    (ref, res) = create_table_with_invalid_replicas(table_name)

    yield (ref, res)

    # Delete the k8s resource if it still exists
    if k8s.get_resource_exists(ref):
        k8s.delete_custom_resource(ref)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)


@service_marker
@pytest.mark.canary
class TestTableReplicas:
    def table_exists(self, table_name: str) -> bool:
        return table.get(table_name) is not None

    def test_create_table_with_replica(self, table_with_replicas):
        (ref, res) = table_with_replicas

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Wait for the table to be active
        table.wait_until(
            table_name,
            table.status_matches("ACTIVE"),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=15,
        )

        # Wait for the replica to be created
        table.wait_until(
            table_name,
            table.replicas_match([REPLICA_REGION_1]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=15,
        )

        # Wait for the replica to be active
        table.wait_until(
            table_name,
            table.replica_status_matches(REPLICA_REGION_1, "ACTIVE"),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=15,
        )

        # Verify the replica exists
        replicas = table.get_replicas(table_name)
        assert replicas is not None
        assert len(replicas) == 1
        assert replicas[0]["RegionName"] == REPLICA_REGION_1
        assert replicas[0]["ReplicaStatus"] == "ACTIVE"

    def test_add_replica(self, table_with_replicas):
        (ref, res) = table_with_replicas

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Add a second replica
        cr["spec"]["replicas"] = [
            {
                "regionName": REPLICA_REGION_1
            },
            {
                "regionName": REPLICA_REGION_2
            }
        ]

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        # Wait for the replicas to be updated
        table.wait_until(
            table_name,
            table.replicas_match([REPLICA_REGION_1, REPLICA_REGION_2]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=15,
        )

        # Wait for the new replica to be active
        table.wait_until(
            table_name,
            table.replica_status_matches(REPLICA_REGION_2, "ACTIVE"),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=15,
        )

        # Verify both replicas exist
        replicas = table.get_replicas(table_name)
        assert replicas is not None
        assert len(replicas) == 2

        region_names = [r["RegionName"] for r in replicas]
        assert REPLICA_REGION_1 in region_names
        assert REPLICA_REGION_2 in region_names

        for replica in replicas:
            assert replica["ReplicaStatus"] == "ACTIVE"

    def test_remove_replica(self, table_with_replicas):
        (ref, res) = table_with_replicas

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Remove the second replica
        cr["spec"]["replicas"] = [
            replica for replica in cr["spec"]["replicas"]
            if replica["regionName"] != REPLICA_REGION_2
        ]

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        # Wait for the replica to be removed
        table.wait_until(
            table_name,
            table.replicas_match([REPLICA_REGION_1]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=15,
        )

        # Verify only one replica exists
        replicas = table.get_replicas(table_name)
        assert replicas is not None
        assert len(replicas) == 1
        assert replicas[0]["RegionName"] == REPLICA_REGION_1

    def test_delete_table_with_replicas(self, table_with_replicas):
        (ref, res) = table_with_replicas

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Delete the k8s resource
        k8s.delete_custom_resource(ref)

        max_wait_seconds = REPLICA_WAIT_AFTER_SECONDS
        interval_seconds = 15
        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            if not self.table_exists(table_name):
                break
            time.sleep(interval_seconds)

        # Verify the table was deleted
        assert not self.table_exists(table_name)

    def test_terminal_condition_for_invalid_stream_specification(self, table_with_invalid_replicas):
        (ref, res) = table_with_invalid_replicas

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Wait for the terminal condition to be set
        max_wait_seconds = 60
        interval_seconds = 5
        start_time = time.time()
        terminal_condition_set = False

        while time.time() - start_time < max_wait_seconds:
            cr = k8s.get_resource(ref)
            if cr and "status" in cr and "conditions" in cr["status"]:
                for condition_obj in cr["status"]["conditions"]:
                    if condition_obj["type"] == "ACK.Terminal" and condition_obj["status"] == "True":
                        terminal_condition_set = True
                        # Verify the error message
                        assert "table must have DynamoDB Streams enabled with StreamViewType set to NEW_AND_OLD_IMAGES" in condition_obj[
                            "message"]
                        break

            if terminal_condition_set:
                break

            time.sleep(interval_seconds)

        assert terminal_condition_set, "Terminal condition was not set for invalid StreamSpecification"