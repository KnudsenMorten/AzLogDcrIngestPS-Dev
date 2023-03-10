Function CreateUpdate-AzLogAnalyticsCustomLogTableDcr
{
  <#
    .SYNOPSIS
    Create or Update Azure LogAnalytics Custom Log table - used together with Data Collection Rules (DCR)
    for Log Ingestion API upload to LogAnalytics

    .DESCRIPTION
    Uses schema based on source object

    .PARAMETER Tablename
    Specifies the table name in LogAnalytics

    .PARAMETER SchemaSourceObject
    This is the schema in hash table format coming from the source object

    .PARAMETER AzLogWorkspaceResourceId
    This is the Loganaytics Resource Id

    .PARAMETER AzAppId
    This is the Azure app id og an app with Contributor permissions in LogAnalytics + Resource Group for DCRs
        
    .PARAMETER AzAppSecret
    This is the secret of the Azure app

    .PARAMETER TenantId
    This is the Azure AD tenant id

    .INPUTS
    None. You cannot pipe objects

    .OUTPUTS
    Output of REST PUT command. Should be 200 for success

    .EXAMPLE
    PS> CreateUpdate-AzLogAnalyticsCustomLogTableDcr -AzLogWorkspaceResourceId $AzLogWorkspaceResourceId -SchemaSourceObject $Schema -TableName $TableName `
                                                     -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose 
 #>

    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter(mandatory)]
                [array]$SchemaSourceObject,
            [Parameter(mandatory)]
                [string]$AzLogWorkspaceResourceId,
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )


    #--------------------------------------------------------------------------
    # Connection
    #--------------------------------------------------------------------------
        $Headers = Get-AzAccessTokenManagement -AzAppId $AzAppId `
                                               -AzAppSecret $AzAppSecret `
                                               -TenantId $TenantId -Verbose:$Verbose

    #--------------------------------------------------------------------------
    # LogAnalytics Table check
    #--------------------------------------------------------------------------

        $Table         = $TableName  + "_CL"    # TableName with _CL (CustomLog)

        If ($Table.Length -gt 45)
            {
                Write-Error "ERROR - Reduce length of tablename, as it has a maximum of 45 characters (current length: $($Table.Length))"
            }

    #--------------------------------------------------------------------------
    # Creating/Updating LogAnalytics Table based upon data source schema
    #--------------------------------------------------------------------------

		# automatic patching of 
		$tableBodyPatch = @{
								properties = @{
												schema = @{
																name    = $Table
																columns = @($Changes)
															}
											}
						   } | ConvertTo-Json -Depth 10

        $tableBodyPut   = @{
                                properties = @{
                                                schema = @{
                                                                name    = $Table
                                                                columns = @($SchemaSourceObject)
                                                            }
                                            }
                           } | ConvertTo-Json -Depth 10

        # create/update table schema using REST
        $TableUrl = "https://management.azure.com" + $AzLogWorkspaceResourceId + "/tables/$($Table)?api-version=2021-12-01-preview"

        Try
            {
                Write-Verbose ""
                Write-Verbose "Trying to update existing LogAnalytics table schema for table [ $($Table) ] in "
                Write-Verbose $AzLogWorkspaceResourceId

                Invoke-WebRequest -Uri $TableUrl -Method Patch -Headers $Headers -Body $TablebodyPatch
            }
        Catch
            {

            $Result = Invoke-WebRequest -Uri $TableUrl -Method PUT -Headers $Headers -Body $TablebodyPut

                Try
                    {
                        Write-Verbose ""
                        Write-Verbose "LogAnalytics Table doesn't exist or problems detected .... creating table [ $($Table) ] in"
                        Write-Verbose $AzLogWorkspaceResourceId

                        Invoke-WebRequest -Uri $TableUrl -Method PUT -Headers $Headers -Body $TablebodyPut
                    }
                catch
                    {
                        $FailureMessage = $_.Exception.Message
                        $ErrorDetails = $_.ErrorDetails.Message
                        Write-Error ""
                        write-Error $FailureMessage
                        Write-Error ""
                        write-Error $ErrorDetails
                        Write-Error ""
                        Write-Error "Something went wrong .... recreating table [ $($Table) ] in"
                        Write-Error $AzLogWorkspaceResourceId

                        Invoke-WebRequest -Uri $TableUrl -Method DELETE -Headers $Headers
                                
                        Start-Sleep -Seconds 10
                                
                        Invoke-WebRequest -Uri $TableUrl -Method PUT -Headers $Headers -Body $TablebodyPut
                    }
            }

        
        return
}
