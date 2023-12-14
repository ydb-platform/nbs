variable "iam_token" {
    type = string
}

variable "cloud_id" {
    type = string
    default = "bjeh01oag2p6ebe3ls69"
}

variable "zone" {
    type = string
    default = "eu-north1-c"
}

variable "nbs-github-sa-roles-list" {
    type = list(string)
    default = [ "compute.admin" ]
}