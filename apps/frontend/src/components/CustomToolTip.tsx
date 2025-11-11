import { Box, Tooltip } from '@mantine/core'
import React from 'react'

const CustomToolTip = ({label,children}:{label:string,children:React.ReactNode}) => {
  return (
    <Box>
        <Tooltip events={{hover:true,focus:false,touch:true}} label={label}>
        {children}      
      </Tooltip>
    </Box>
  )
}

export default CustomToolTip
