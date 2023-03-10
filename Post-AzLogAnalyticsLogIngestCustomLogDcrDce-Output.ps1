Function Post-AzLogAnalyticsLogIngestCustomLogDcrDce-Output
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter(mandatory)]
                [string]$DcrName,
            [Parameter(mandatory)]
                [string]$DceName,
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter()]
                [string]$BatchAmount,
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )


        $AzDcrDceDetails = Get-AzDcrDceDetails -DcrName $DcrName -DceName $DceName `
                                               -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose

        Post-AzLogAnalyticsLogIngestCustomLogDcrDce  -DceUri $AzDcrDceDetails[2] -DcrImmutableId $AzDcrDceDetails[6] -TableName $TableName `
                                                     -DcrStream $AzDcrDceDetails[7] -Data $Data -BatchAmount $BatchAmount `
                                                     -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose
        
        # Write result to screen
        $DataVariable | Out-String | Write-Verbose 
}

