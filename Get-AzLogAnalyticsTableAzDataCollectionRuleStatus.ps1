Function Get-AzLogAnalyticsTableAzDataCollectionRuleStatus
{
 <#
    .SYNOPSIS
    Get status about Azure Loganalytics tables and Data Collection Rule.

    .DESCRIPTION
    Used to detect if table7DCR must be create/updated - or it is valid to send in data

    .PARAMETER Data
    This is the data array

    .INPUTS
    None. You cannot pipe objects

    .OUTPUTS
	TRUE means existing environment must be updated - or table/DCR must be created
	FALSE means everything is ok including schema - next step is to post data
	
    .EXAMPLE
	$Schema = Get-ObjectSchemaAsArray -Data $Data
	$StructureCheck = Get-AzLogAnalyticsTableAzDataCollectionRuleStatus -AzLogWorkspaceResourceId $AzLogWorkspaceResourceId -TableName $TableName -DcrName $DcrName -SchemaSourceObject $Schema `
																		-AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose
 #>

    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$AzLogWorkspaceResourceId,
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter(mandatory)]
                [string]$DcrName,
            [Parameter(mandatory)]
                [array]$SchemaSourceObject,
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )


    Write-Verbose "  Checking LogAnalytics table and Data Collection Rule configuration .... Please Wait !"

    # by default ($false)
    $AzDcrDceTableCustomLogCreateUpdate = $false     # $True/$False - typically used when updates to schema detected

    #--------------------------------------------------------------------------
    # Connection
    #--------------------------------------------------------------------------

        $Headers = Get-AzAccessTokenManagement -AzAppId $AzAppId `
                                               -AzAppSecret $AzAppSecret `
                                               -TenantId $TenantId -Verbose:$Verbose

        #--------------------------------------------------------------------------
        # Check if Azure LogAnalytics Table exist
        #--------------------------------------------------------------------------

            $TableUrl = "https://management.azure.com" + $AzLogWorkspaceResourceId + "/tables/$($TableName)_CL?api-version=2021-12-01-preview"
            $TableStatus = Try
                                {
                                    Invoke-RestMethod -Uri $TableUrl -Method GET -Headers $Headers
                                }
                           Catch
                                {
                                    Write-Verbose "  LogAnalytics table wasn't found !"
                                    # initial setup - force to auto-create structure
                                    $AzDcrDceTableCustomLogCreateUpdate = $true     # $True/$False - typically used when updates to schema detected
                                }

        #--------------------------------------------------------------------------
        # Compare schema between source object schema and Azure LogAnalytics Table
        #--------------------------------------------------------------------------

            If ($TableStatus)
                {
                    $CurrentTableSchema = $TableStatus.properties.schema.columns

                    # Checking number of objects in schema
                        $CurrentTableSchemaCount = $CurrentTableSchema.count
                        $SchemaSourceObjectCount = ($SchemaSourceObject.count) + 1  # add 1 because TimeGenerated will automatically be added

                        If ($SchemaSourceObjectCount -gt $CurrentTableSchemaCount)
                            {
                               Write-Verbose "  Schema mismatch - Schema source object contains more properties than defined in current schema"
                               $AzDcrDceTableCustomLogCreateUpdate = $true     # $True/$False - typically used when updates to schema detected
                            }

                    # Verify LogAnalytics table schema matches source object ($SchemaSourceObject) - otherwise set flag to update schema in LA/DCR
<#
                        ForEach ($Entry in $SchemaSourceObject)
                            {
                                $ChkSchema = $CurrentTableSchema | Where-Object { ($_.name -eq $Entry.name) -and ($_.type -eq $Entry.type) }

                                If ($ChkSchema -eq $null)
                                    {
                                        Write-Verbose "  Schema mismatch - property missing or different type (name: $($Entry.name), type: $($Entry.type))"
                                        # Set flag to update schema
                                        $AzDcrDceTableCustomLogCreateUpdate = $true     # $True/$False - typically used when updates to schema detected
                                    }
                            }
#>
                }

        #--------------------------------------------------------------------------
        # Check if Azure Data Collection Rule exist
        #--------------------------------------------------------------------------

            # Check in global variable
            $DcrInfo = $global:AzDcrDetails | Where-Object { $_.name -eq $DcrName }
                If (!($DcrInfo))
                    {
                        Write-Verbose "  DCR was not found [ $($DcrName) ]"
                        # initial setup - force to auto-create structure
                        $AzDcrDceTableCustomLogCreateUpdate = $true     # $True/$False - typically used when updates to schema detected
                    }

            If ($AzDcrDceTableCustomLogCreateUpdate -eq $false)
                {
                    Write-Verbose "  Success - Schema & DCR structure is OK"
                }

        Return $AzDcrDceTableCustomLogCreateUpdate
}
