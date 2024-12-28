data "aws_iam_policy_document" "glue_scripts_s3_policy" {
  statement {
    effect = "Allow"
    actions = [
      "sts:GetCallerIdentity"
    ]
    resources = ["*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      "arn:aws:s3:::${var.glue_script_bucket}"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject"
    ]
    resources = [
      "arn:aws:s3:::${var.glue_script_bucket}/*"
    ]
  }
}

resource "aws_iam_policy" "glue_scripts_s3_policy" {
  name        = "GlueScriptsS3Policy"
  description = "Policy for accessing Glue script S3 bucket"
  policy      = data.aws_iam_policy_document.glue_scripts_s3_policy.json
}

resource "aws_iam_user" "github_action_user" {
  name = "github-action"
}

resource "aws_iam_access_key" "github_action_user_key" {
  user = aws_iam_user.github_action_user.name
}

resource "aws_iam_user_policy_attachment" "github_action_user_glue_scripts_policy" {
  user       = aws_iam_user.github_action_user.name
  policy_arn = aws_iam_policy.glue_scripts_s3_policy.arn
}
