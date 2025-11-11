import { Group, Menu, MenuProps, Text } from "@mantine/core";
import classes from "styles/menu.module.css";
import { IconChevronRight } from "@tabler/icons-react";

const EmployerFreelancerDirectoryNavMenu = ({ ...props }: MenuProps) => {
  return (
    <>
      <Menu
        trigger="hover"
        styles={{
          dropdown: {
            borderColor: "var(--mantine-color-primaryPurple-3)",
            backgroundColor: "var(--mantine-color-primaryBackground-9)",
          },
        }}
        position="right"
        {...props}
      >
        <Menu.Target>
          <Group
            px={12}
            py={6.7}
            gap={6}
            c="primaryPurple.3"
            className={classes.menuBar}
            fz={14}
            style={{ cursor: "pointer", borderRadius: 4 }}
          >
            <Text fz={16}>Freelancer</Text>
            <IconChevronRight size={"18"} />
          </Group>
        </Menu.Target>
      </Menu>
    </>
  );
};

export default EmployerFreelancerDirectoryNavMenu;
