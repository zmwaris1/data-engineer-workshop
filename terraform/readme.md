Terraform commands for configuring your cloud service:

```
terrraform init
terraform plan
terraform apply
```

the files that are required to define you cloud service credentials:

`main.tf`
`variables.tf`

`main.tf` contains all the configuration for the services you want in your cloud account.
`variables.tf` contains the details of the services of the cloud.

After you have configured you terraform files and added the credentials for you cloud account you can use the 
above mentioned terraform commands to fire up the services in your cloud account

If you want to end any service in your cloud account you should use:

`terraform destroy`