data "aws_iam_policy_document" "github_oidc_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = ["arn:aws:iam::982534349340:oidc-provider/token.actions.githubusercontent.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [
        "repo:WikarNotAvailable/BDP-Blockchain-Data-Processing:ref:refs/heads/develop",
        "repo:WikarNotAvailable/BDP-Blockchain-Data-Processing:ref:refs/heads/master"]
    }
  }
}

resource "aws_iam_role" "github_oidc_workflow_role" {
  name               = var.github_role_name
  assume_role_policy = data.aws_iam_policy_document.github_oidc_assume_role_policy.json
}

resource "aws_iam_role_policy_attachment" "github_oidc_s3_full_access" {
  role       = aws_iam_role.github_oidc_workflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "github_oidc_cloudformation_full_access" {
  role       = aws_iam_role.github_oidc_workflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSCloudFormationFullAccess"
}

resource "aws_iam_role_policy_attachment" "github_oidc_codebuild_developer_access" {
  role       = aws_iam_role.github_oidc_workflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSCodeBuildDeveloperAccess"
}

resource "aws_iam_role_policy_attachment" "github_oidc_codedeploy_full_access" {
  role       = aws_iam_role.github_oidc_workflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSCodeDeployFullAccess"
}

resource "aws_iam_role_policy_attachment" "github_oidc_lambda_full_access" {
  role       = aws_iam_role.github_oidc_workflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSLambda_FullAccess"
}

resource "aws_iam_role_policy_attachment" "github_oidc_secretsmanager_read_write" {
  role       = aws_iam_role.github_oidc_workflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
}

data "aws_iam_policy_document" "glue_scripts_access" {
  statement {
    effect = "Allow"
    actions = [
      "sts:AssumeRoleWithWebIdentity"
    ]
    resources = [
      "arn:aws:iam::982534349340:role/${var.github_role_name}"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:PutObject"
    ]
    resources = [
      "arn:aws:s3:::${var.glue_script_bucket}",
      "arn:aws:s3:::${var.glue_script_bucket}/*"
    ]
  }
}

resource "aws_iam_role_policy" "glue_scripts_inline" {
  name   = "glue-scripts-access"
  role   = aws_iam_role.github_oidc_workflow_role.name
  policy = data.aws_iam_policy_document.glue_scripts_access.json
}