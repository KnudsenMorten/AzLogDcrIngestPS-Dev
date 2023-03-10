Function CheckCreateUpdate-TableDcr-Structure
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter(mandatory)]
                [string]$AzLogWorkspaceResourceId,
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter(mandatory)]
                [string]$DcrName,
            [Parameter(mandatory)]
                [string]$DceName,
            [Parameter(mandatory)]
                [string]$LogIngestServicePricipleObjectId,
            [Parameter(mandatory)]
                [string]$AzDcrSetLogIngestApiAppPermissionsDcrLevel,
            [Parameter(mandatory)]
                [boolean]$AzLogDcrTableCreateFromAnyMachine,
            [Parameter(mandatory)]
                [AllowEmptyCollection()]
                [array]$AzLogDcrTableCreateFromReferenceMachine,
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )


    #-------------------------------------------------------------------------------------------
    # Create/Update Schema for LogAnalytics Table & Data Collection Rule schema
    #-------------------------------------------------------------------------------------------

        If ( ($AzAppId) -and ($AzAppSecret) )
            {
                #-----------------------------------------------------------------------------------------------
                # Check if table and DCR exist - or schema must be updated due to source object schema changes
                #-----------------------------------------------------------------------------------------------
                    
                    # Get insight about the schema structure
                    $Schema = Get-ObjectSchemaAsArray -Data $Data
                    $StructureCheck = Get-AzLogAnalyticsTableAzDataCollectionRuleStatus -AzLogWorkspaceResourceId $AzLogWorkspaceResourceId -TableName $TableName -DcrName $DcrName -SchemaSourceObject $Schema `
                                                                                        -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose

                #-----------------------------------------------------------------------------------------------
                # Structure check = $true -> Create/update table & DCR with necessary schema
                #-----------------------------------------------------------------------------------------------

                    If ($StructureCheck -eq $true)
                        {
                            If ( ( $env:COMPUTERNAME -in $AzLogDcrTableCreateFromReferenceMachine) -or ($AzLogDcrTableCreateFromAnyMachine -eq $true) )    # manage table creations
                                {
                                    
                                    # build schema to be used for LogAnalytics Table
                                    $Schema = Get-ObjectSchemaAsHash -Data $Data -ReturnType Table -Verbose:$Verbose

                                    CreateUpdate-AzLogAnalyticsCustomLogTableDcr -AzLogWorkspaceResourceId $AzLogWorkspaceResourceId -SchemaSourceObject $Schema -TableName $TableName `
                                                                                 -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose 


                                    # build schema to be used for DCR
                                    $Schema = Get-ObjectSchemaAsHash -Data $Data -ReturnType DCR

                                    CreateUpdate-AzDataCollectionRuleLogIngestCustomLog -AzLogWorkspaceResourceId $AzLogWorkspaceResourceId -SchemaSourceObject $Schema `
                                                                                        -DceName $DceName -DcrName $DcrName -TableName $TableName `
                                                                                        -LogIngestServicePricipleObjectId $LogIngestServicePricipleObjectId `
                                                                                        -AzDcrSetLogIngestApiAppPermissionsDcrLevel $AzDcrSetLogIngestApiAppPermissionsDcrLevel `
                                                                                        -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose
                                }
                        }
                } # create table/DCR
}

