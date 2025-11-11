import { Group, Text, GroupProps, TextProps, Tooltip } from "@mantine/core";

const IconWithLabel = ({
  icon,
  label,
  fontProps,
  ...props
}: {
  icon: React.ReactNode;
  label: string;
  fontProps?: TextProps;
} & GroupProps) => {
  return (
    <Group gap={12} {...props}>
      {icon}
      <Tooltip label={label}>
        <Text
          fz={{ base: 14, md: 16, xl: 18 }}
          lh="1.17"
          td="none"
          c="white"
          tt="capitalize"
          maw={200}
          truncate
          {...fontProps}
        >
          {label}
        </Text>
      </Tooltip>
    </Group>
  );
};
export default IconWithLabel;
