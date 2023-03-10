Function Convert-CimArrayToObjectFixStructure
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data
         )

    Write-Verbose "  Converting CIM array to Object & removing CIM class data in array .... please wait !"

    # Convert from array to object
    $Object = $Data | ConvertTo-Json -Depth 20 | ConvertFrom-Json 

    # remove CIM info columns from object
    $ObjectModified = $Object | Select-Object -Property * -ExcludeProperty CimClass, CimInstanceProperties, CimSystemProperties

    return $ObjectModified
}

