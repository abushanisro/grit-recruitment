import { Badge, BadgeProps } from "@mantine/core";
import { forwardRef } from "react";

const CustomMediumBadge = forwardRef<
  HTMLDivElement,
  { label: string } & BadgeProps
>(({ label, ...props }, ref) => {
  return (
    <Badge
      ref={ref}
      bg="customBlack.1"
      lh="1.17"
      fw={400}
      fz={14}
      tt="none"
      pos="absolute"
      top={-31}
      left={30}
      style={{
        borderStartStartRadius: 8,
        borderStartEndRadius: 8,
        borderEndEndRadius: 0,
        borderEndStartRadius: 0,
      }}
      py={14}
      {...props}
    >
      {label}
    </Badge>
  );
});
CustomMediumBadge.displayName = "CustomMediumBadge";
export default CustomMediumBadge;
