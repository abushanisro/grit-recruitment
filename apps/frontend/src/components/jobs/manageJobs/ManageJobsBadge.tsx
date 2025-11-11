import { Badge, BadgeProps } from "@mantine/core";
import { forwardRef } from "react";

const ManageJobsBadge = forwardRef<
  HTMLDivElement,
  { label: string } & BadgeProps
>(({ label, ...props }, ref) => {
  return (
    <Badge
      ref={ref}
      bg="transparent"
      lh="1.17"
      fw={400}
      tt="none"
      pos="absolute"
      right={0}
      fz={{ base: 14 }}
      top={{ base: "30px", sm: "30px", xl: "32px" }}
      p={10}
      {...props}
    >
      {label}
    </Badge>
  );
});
ManageJobsBadge.displayName = "ManageJobsBadge";
export default ManageJobsBadge;
