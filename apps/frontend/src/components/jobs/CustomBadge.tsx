import { Badge, BadgeProps } from "@mantine/core";
import { forwardRef } from "react";

const CustomBadge = forwardRef<HTMLDivElement, { label: string } & BadgeProps>(
  ({ label, ...props }, ref) => {
    return (
      <Badge
        ref={ref}
        bg="customBlack.1"
        fz={{ base: 14, sm: 16 }}
        lh="1.17"
        fw={400}
        tt="none"
        {...props}
      >
        {label}
      </Badge>
    );
  }
);
CustomBadge.displayName = "CustomBadge";
export default CustomBadge;
