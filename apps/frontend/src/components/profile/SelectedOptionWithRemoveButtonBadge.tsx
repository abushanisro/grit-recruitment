import { Badge, BadgeProps, CloseButton, Group } from "@mantine/core";

const SelectedOptionWithRemoveButtonBadge = ({
  label,
  onClick,
  ...props
}: { label: string; onClick: () => void } & BadgeProps) => {
  return (
    <Badge
      bg="primaryGrey.1"
      c="white"
      px={16}
      py={16}
      onClick={onClick}
      tt="capitalize"
      fz={{ base: 14, sm: 16 }}
      fw={600}
      {...props}
    >
      <Group gap={4}>
        {label}
        <CloseButton c="white" variant="transparent" size="xs" />
      </Group>
    </Badge>
  );
};
export default SelectedOptionWithRemoveButtonBadge;
