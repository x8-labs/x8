from botocore.exceptions import ClientError


def ensure_launch_template(
    ec2_client,
    launch_template_name: str,
    ami_id: str,
    instance_type: str,
    security_group_id: str,
    user_data: str,
    instance_profile_arn: str | None = None,
    keypair_name: str | None = None,
    subnet_id: str | None = None,
) -> tuple[str, str]:
    try:
        lt = ec2_client.describe_launch_templates(
            LaunchTemplateNames=[launch_template_name]
        )["LaunchTemplates"][0]
        return lt["LaunchTemplateId"], "$Latest"
    except ClientError as e:
        if (
            e.response["Error"]["Code"]
            != "InvalidLaunchTemplateName.NotFoundException"
        ):
            raise

    # Create fresh
    launch_template_data = {
        "ImageId": ami_id,
        "InstanceType": instance_type,
        "SecurityGroupIds": [security_group_id],
        "UserData": user_data,
    }

    if instance_profile_arn:
        launch_template_data["IamInstanceProfile"] = {
            "Arn": instance_profile_arn  # type: ignore
        }

    if keypair_name:
        launch_template_data["KeyName"] = keypair_name

    if subnet_id:
        launch_template_data["SubnetId"] = subnet_id

    resp = ec2_client.create_launch_template(
        LaunchTemplateName=launch_template_name,
        LaunchTemplateData=launch_template_data,
    )
    lt = resp["LaunchTemplate"]
    print(f"Created launch template: {launch_template_name}")
    return lt["LaunchTemplateId"], "$Latest"


def delete_launch_template(
    ec2_client,
    launch_template_name: str,
) -> None:
    try:
        ec2_client.delete_launch_template(
            LaunchTemplateName=launch_template_name
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in (
            "InvalidLaunchTemplateId.NotFound",
            "InvalidLaunchTemplateName.NotFoundException",
        ):
            return

        else:
            raise


def auto_detect_network_config(
    ec2_client,
    vpc_id: str | None = None,
    subnet_ids: list[str] = [],
) -> tuple[str | None, list[str] | None]:
    """Auto-detect VPC and subnet configuration."""
    vpc_id = vpc_id
    subnet_ids = subnet_ids
    try:
        if not vpc_id:
            # Get default VPC
            vpcs = ec2_client.describe_vpcs(
                Filters=[{"Name": "is-default", "Values": ["true"]}]
            )
            if vpcs["Vpcs"]:
                vpc_id = vpcs["Vpcs"][0]["VpcId"]

        if not subnet_ids and vpc_id:
            # Get subnets in the detected VPC
            subnets = ec2_client.describe_subnets(
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
            )
            subnet_ids = [subnet["SubnetId"] for subnet in subnets["Subnets"]]

    except Exception as e:
        print(f"Warning: Could not auto-detect network config: {e}")

    return vpc_id, subnet_ids
