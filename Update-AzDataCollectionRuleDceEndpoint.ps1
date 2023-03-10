Function Update-AzDataCollectionRuleDceEndpoint
{
 <#
    .SYNOPSIS
    Updates the DceEndpointUri of the Data Collection Rule

    .DESCRIPTION
    Used to change the Data Collection Endpoint in a Data Collection Rule

    .PARAMETER DcrResourceId
    This is resource id of the Data Collection Rule which should be changed

    .PARAMETER DceResourceId
    This is resource id of the Data Collection Endpoint to change to (target)

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
    PS> Update-AzDataCollectionRuleDceEndpoint

 #>

    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$DcrResourceId,
            [Parameter(mandatory)]
                [string]$DceResourceId,
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
    # get existing DCR
    #--------------------------------------------------------------------------

        $DcrUri = "https://management.azure.com" + $DcrResourceId + "?api-version=2022-06-01"
        $DCR = Invoke-RestMethod -Uri $DcrUri -Method GET -Headers $headers

    #--------------------------------------------------------------------------
    # update payload object
    #--------------------------------------------------------------------------

        $DCR.properties.dataCollectionEndpointId = $DceResourceId

    #--------------------------------------------------------------------------
    # update existing DCR
    #--------------------------------------------------------------------------

        Write-host "Updating DCE EndpointId for DCR"
        Write-host $DcrResourceId

        # convert modified payload to JSON-format
        $DcrPayload = $Dcr | ConvertTo-Json -Depth 20

        # update changes to existing DCR
        $DcrUri = "https://management.azure.com" + $DcrResourceId + "?api-version=2022-06-01"
        $DCR = Invoke-RestMethod -Uri $DcrUri -Method PUT -Body $DcrPayload -Headers $Headers

}

