import React, { ChangeEvent, useEffect, useRef, useState } from "react";
import { Box, Text, Stack, Group, Button } from "@mantine/core";

import {
  FieldValues,
  Path,
  RegisterOptions,
  UseFormReturn,
} from "react-hook-form";
import { IconUpload } from "@tabler/icons-react";

export default function JobDragAndDrop<T extends FieldValues>({
  hForm,
  name,
  rules,
  acceptedFileTypes,
  onFileSelect,
  removeFile,
}: {
  hForm: UseFormReturn<T>;
  name: Path<T>;
  rules?: RegisterOptions;
  acceptedFileTypes: string;
  onFileSelect: (file: File) => void;
  removeFile: () => void;
}) {
  const [isDragging, setIsDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const { register, watch } = hForm;
  const selectedFile = watch(name);

  useEffect(() => {
    register(name, { required: rules?.required });
  }, []);

  const handlePickFile = (e: ChangeEvent<HTMLInputElement>) => {
    const target = e.target;

    if (!e.target.files) {
      return;
    }
    onFileSelect(e.target.files[0]);
    target.value = "";
  };

  const handleDragIn = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.dataTransfer.items && e.dataTransfer.items.length > 0) {
      setIsDragging(true);
    }
  };
  const handleDragOut = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  };
  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      onFileSelect(e.dataTransfer.files[0]);
    }
  };
  const browseImage = () => {
    return (
      <label
        style={{
          fontSize: "md",
          fontWeight: "semibold",
          lineHeight: "24px",
          color: "white",
        }}
      >
        Browse
        <input
          ref={inputRef}
          type="file"
          style={{ display: "none" }}
          accept={acceptedFileTypes}
          onChange={handlePickFile}
        />
      </label>
    );
  };
  return (
    <>
      <Box
        h={150}
        style={{
          borderRadius: 10,
          border: isDragging ? "2px solid #9FB1BD" : "2px dashed #9FB1BD",
          cursor: "pointer",
        }}
        onDrop={handleDrop}
        onDragLeave={handleDragOut}
        onDragEnter={handleDragIn}
        onDragOver={(e) => {
          e.preventDefault();
          e.stopPropagation();
        }}
        onClick={() => {
          inputRef?.current?.click();
        }}
        px={4}
      >
        <Stack
          w="100%"
          h="100%"
          gap={4}
          justify="center"
          align="center"
          style={{ pointerEvents: "none" }}
        >
          <Box c="#9FB1BD">
            <IconUpload size={50} color="currentColor" />
          </Box>
          <Text fz="md" fw="semibold" lh="24px" c="gray" ta="center">
            Drag and Drop your file here or {browseImage()}
          </Text>
        </Stack>
      </Box>

      {selectedFile && (
        <Group
          my={10}
          px={20}
          bg="#9FB1BD"
          style={{ borderRadius: 10, overflow: "hidden" }}
          fz={{ base: 13, sm: 14 }}
          fw="normal"
          lh="1.21"
          color="white"
          align="center"
          justify="space-between"
        >
          <Text c="black">{selectedFile.name}</Text>

          <Button pr={0} color="#9FB1BD" onClick={removeFile}>
            <Text fz={12} c="black">
              X
            </Text>
          </Button>
        </Group>
      )}
    </>
  );
}
